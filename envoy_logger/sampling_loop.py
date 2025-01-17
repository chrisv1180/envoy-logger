from datetime import datetime, date
import time
from typing import List, Dict
import logging
from requests.exceptions import ReadTimeout, ConnectTimeout

from influxdb_client import WritePrecision, InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from . import envoy
from .model import SampleData, PowerSample, InverterSample, filter_new_inverter_data, BatteriesSample
from .cfg import Config

class SamplingLoop:
    interval = 10  # seconds
    interval_battery = 6  # each (interval_battery * interval) seconds one measurement
    interval_battery_counter = interval_battery  # at least as high as interval_battery or first value is delayed

    def __init__(self, token: str, cfg: Config) -> None:
        self.cfg = cfg
        self.session_id = envoy.login(self.cfg.envoy_url, token)

        influxdb_client = InfluxDBClient(
            url=cfg.influxdb_url,
            token=cfg.influxdb_token,
            org=cfg.influxdb_org
        )
        self.influxdb_write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)
        self.influxdb_query_api = influxdb_client.query_api()

        # Used to track the transition to the next day for daily measurements
        self.todays_date = date.today()

         # Used to track the transition to the next hour of hourly measurements
        self.actual_hour = datetime.now().hour

        self.prev_inverter_data = None

    def run(self):
        timeout_count = 0
        while True:
            try:
                data = self.get_sample()
                inverter_data = self.get_inverter_data()
                battery_data = self.get_battery_data()
            except (ReadTimeout, ConnectTimeout) as e:
                # Envoy gets REALLY MAD if you block it's access to enphaseenergy.com
                # using a VLAN.
                # It's software gets hung up for some reason, and some requests will stall.
                # Allow envoy requests to timeout (and skip this sample iteration)
                timeout_count += 1
                logging.warning("Envoy request timed out (%d/10)", timeout_count)
                if timeout_count >= 10:
                    # Give up after a while
                    raise
                pass
            else:
                try:
                    self.write_to_influxdb(data, inverter_data, battery_data)
                    self.write_to_influxdb_hourly()
                    timeout_count = 0
                except Exception as e:
                    logging.warning(f"write_to_influxdb had an exception ({timeout_count + 1}/10): {e}")
                    timeout_count += 1
                    if timeout_count >= 10:
                        # Give up after a while
                        raise
                    # sleep for a minute (50s + 10s wait in get sample) to allow system to recover
                    time.sleep(50)


    def get_sample(self) -> SampleData:
        # Determine how long until the next sample needs to be taken
        now = datetime.now()
        time_to_next = self.interval - (now.timestamp() % self.interval)

        # wait!
        time.sleep(time_to_next)

        data = envoy.get_power_data(self.cfg.envoy_url, self.session_id)

        return data

    def get_inverter_data(self) -> Dict[str, InverterSample]:
        data = envoy.get_inverter_data(self.cfg.envoy_url, self.session_id)

        if self.prev_inverter_data is None:
            self.prev_inverter_data = data
            # Hard to know how stale inverter data is, so discard this sample
            # since I have nothing to compare to yet
            return {}

        # filter out stale inverter samples
        filtered_data = filter_new_inverter_data(data, self.prev_inverter_data)
        if filtered_data:
            logging.debug("Got %d unique inverter measurements", len(filtered_data))
        self.prev_inverter_data = data
        return filtered_data

    def get_battery_data(self) -> BatteriesSample:
        data = None
        if self.interval_battery_counter < self.interval_battery - 1:
            self.interval_battery_counter = self.interval_battery_counter + 1
        else:
            data = envoy.get_battery_data(self.cfg.envoy_url, self.session_id)
            self.interval_battery_counter = 0
        return data

    def write_to_influxdb(self, data: SampleData, inverter_data: Dict[str, InverterSample], batteries: BatteriesSample) -> None:
        hr_points = self.get_high_rate_points(data, inverter_data, batteries)
        self.influxdb_write_api.write(bucket=self.cfg.influxdb_bucket_hr, record=hr_points)

    def write_to_influxdb_hourly(self) -> None:
        # if hourly data has to be calculated and hour roled over -> calculate hourly data
        new_hour = datetime.now().hour
        if self.cfg.calc_hourly_data and not (self.actual_hour == new_hour):
            # it is a new hour!
            self.actual_hour = new_hour

            mr_points = self.medium_rate_points()
            if mr_points:
                self.influxdb_write_api.write(bucket=self.cfg.influxdb_bucket_mr, record=mr_points)


        # if daily data has to be calculated and day roled over -> calculate daily data
        new_date = date.today()
        if self.cfg.calc_daily_data and not (self.todays_date == new_date):
            # it is a new day!
            self.todays_date = new_date

            self.write_to_influxdb_daily()

    def write_to_influxdb_daily(self) -> None:
        if self.cfg.calc_daily_data:
            lr_points = self.low_rate_points()
            if lr_points:
                self.influxdb_write_api.write(bucket=self.cfg.influxdb_bucket_lr, record=lr_points)

    def get_high_rate_points(self, data: SampleData, inverter_data: Dict[str, InverterSample], batteries: BatteriesSample) -> List[Point]:
        points = []
        for i, line in enumerate(data.total_consumption.lines):
            p = self.idb_point_from_line("consumption", i, line)
            points.append(p)
        for i, line in enumerate(data.total_production.lines):
            p = self.idb_point_from_line("production", i, line)
            points.append(p)
        for i, line in enumerate(data.net_consumption.lines):
            p = self.idb_point_from_line("net", i, line)
            points.append(p)

        for inverter in inverter_data.values():
            p = self.point_from_inverter(inverter)
            points.append(p)

        if batteries is not None:
            points.extend(self.points_from_batteries(batteries=batteries))

        return points

    def idb_point_from_line(self, measurement_type: str, idx: int, data: PowerSample) -> Point:
        p = Point(f"{measurement_type}-line{idx}")
        p.time(data.ts, WritePrecision.S)
        p.tag("source", self.cfg.source_tag)
        p.tag("measurement-type", measurement_type)
        p.tag("line-idx", idx)

        p.field("P", data.wNow)
        p.field("Q", data.reactPwr)
        p.field("S", data.apprntPwr)

        p.field("I_rms", data.rmsCurrent)
        p.field("V_rms", data.rmsVoltage)

        p.field("whToday", data.whToday)
        p.field("whLifetime", data.whLifetime)

        return p

    def point_from_inverter(self, inverter: InverterSample) -> Point:
        p = Point(f"inverter-production-{inverter.serial}")
        p.time(inverter.ts, WritePrecision.S)
        p.tag("source", self.cfg.source_tag)
        p.tag("measurement-type", "inverter")
        p.tag("serial", inverter.serial)
        self.cfg.apply_tags_to_inverter_point(p, inverter.serial)

        p.field("P", inverter.watts)

        return p

    def points_from_batteries(self, batteries: BatteriesSample) -> list[Point]:
        battery_points = []
        for battery in batteries.batteries:
            p = Point(f"battery-{battery['encharge_capacity']}-{battery['serial_num']}")
            p.time(batteries.ts, WritePrecision.S)
            p.tag("source", self.cfg.source_tag)
            p.tag("measurement-type", "battery")
            p.tag("serial", battery['serial_num'])

            p.field("percentFull", battery['percentFull'])
            p.field("temperature", battery['temperature'])
            p.field("maxCellTemp", battery['maxCellTemp'])
            p.field("led_status", battery['led_status'])

        battery_points.append(p)

        return battery_points

    def low_rate_points(self) -> List[Point]:

        # Collect points that summarize prior day
        points = self.compute_daily_Wh_points()
        points.extend(self.low_rate_points_batteries())
        points.extend(self.compute_daily_Wh_points_balkonkraftwerk())
        points.extend(self.compute_daily_Wh_points_vzlogger())

        return points

    def low_rate_points_batteries(self) -> List[Point]:

        # Collect points that summarize prior day
        points = self.compute_daily_battery_Soc_points()
        points.extend(self.compute_daily_battery_temperature_points())

        return points

    def compute_daily_Wh_points(self) -> List[Point]:
        # Not using integral(interpolate:"linear") since it does not do what you
        # think it would mean. Without the "interoplation" arg, it still does
        # linear interpolation correctly.
        # https://github.com/influxdata/flux/issues/4782
        query = f"""
        from(bucket: "{self.cfg.influxdb_bucket_hr}")
            |> range(start: -24h, stop: 0h)
            |> filter(fn: (r) => r["source"] == "{self.cfg.source_tag}")
            |> filter(fn: (r) => r["_field"] == "P")
            |> integral(unit: 1h)
            |> keep(columns: ["_value", "line-idx", "measurement-type", "serial"])
            |> yield(name: "total")
        """
        result = self.influxdb_query_api.query(query=query)
        unreported_inverters = set(self.cfg.inverters.keys())
        points = []
        for table in result:
            for record in table.records:
                measurement_type = record['measurement-type']
                if measurement_type == "inverter":
                    serial = record['serial']
                    unreported_inverters.discard(serial)
                    p = Point(f"inverter-daily-summary-{serial}")
                    p.tag("serial", serial)
                    self.cfg.apply_tags_to_inverter_point(p, serial)
                else:
                    idx = record['line-idx']
                    p = Point(f"{measurement_type}-daily-summary-line{idx}")
                    p.tag("line-idx", idx)

                #p.time(ts, WritePrecision.S)
                p.tag("source", self.cfg.source_tag)
                p.tag("measurement-type", measurement_type)
                p.tag("interval", "24h")

                p.field("Wh", record.get_value())
                points.append(p)

        # If any inverters did not report in for the day, fill in a 0wh measurement
        for serial in unreported_inverters:
            p = Point(f"inverter-daily-summary-{serial}")
            p.tag("serial", serial)
            self.cfg.apply_tags_to_inverter_point(p, serial)
            #p.time(ts, WritePrecision.S)
            p.tag("source", self.cfg.source_tag)
            p.tag("measurement-type", measurement_type)
            p.tag("interval", "24h")
            p.field("Wh", 0.0)
            points.append(p)

        return points

    def compute_daily_battery_Soc_points(self) -> List[Point]:

        query = f"""
        from(bucket: "{self.cfg.influxdb_bucket_hr}")
            |> range(start: -1d, stop: now())
            |> filter(fn: (r) => r["source"] == "{self.cfg.source_tag}")
            |> filter(fn: (r) => r["measurement-type"] == "battery")
            |> filter(fn: (r) => r["_field"] == "percentFull")
            |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
            |> yield(name: "mean_soc")
        
        from(bucket: "{self.cfg.influxdb_bucket_hr}")
            |> range(start: -1d, stop: now())
            |> filter(fn: (r) => r["source"] == "{self.cfg.source_tag}")
            |> filter(fn: (r) => r["measurement-type"] == "battery")
            |> filter(fn: (r) => r["_field"] == "percentFull")
            |> aggregateWindow(every: 1d, fn: max, createEmpty: false)
            |> yield(name: "max_soc")
        
        from(bucket: "{self.cfg.influxdb_bucket_hr}")
            |> range(start: -1d, stop: now())
            |> filter(fn: (r) => r["source"] == "{self.cfg.source_tag}")
            |> filter(fn: (r) => r["measurement-type"] == "battery")
            |> filter(fn: (r) => r["_field"] == "percentFull")
            |> aggregateWindow(every: 1d, fn: min, createEmpty: false)
            |> yield(name: "min_soc")
        """
        result = self.influxdb_query_api.query(query=query)
        points = []
        for table in result:
            for record in table.records:
                measurement_type = record['measurement-type']
                if measurement_type == "battery":
                    serial = record['serial']
                    p = Point(f"battery-daily-summary-{serial}")
                    p.tag("serial", serial)
                    #p.time(ts, WritePrecision.S)
                    p.tag("source", self.cfg.source_tag)
                    p.tag("measurement-type", measurement_type)
                    p.tag("interval", "24h")

                    p.field(record["result"], record.get_value())
                    points.append(p)

        return points

    def compute_daily_battery_temperature_points(self) -> List[Point]:

        query = f"""
        from(bucket: "{self.cfg.influxdb_bucket_hr}")
            |> range(start: -1d, stop: now())
            |> filter(fn: (r) => r["source"] == "{self.cfg.source_tag}")
            |> filter(fn: (r) => r["measurement-type"] == "battery")
            |> filter(fn: (r) => r["_field"] == "temperature")
            |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
            |> yield(name: "mean_temperature")
        """
        result = self.influxdb_query_api.query(query=query)
        points = []
        for table in result:
            for record in table.records:
                measurement_type = record['measurement-type']
                if measurement_type == "battery":
                    serial = record['serial']
                    p = Point(f"battery-daily-summary-{serial}")
                    p.tag("serial", serial)
                    #p.time(ts, WritePrecision.S)
                    p.tag("source", self.cfg.source_tag)
                    p.tag("measurement-type", measurement_type)
                    p.tag("interval", "24h")

                    p.field(record["result"], record.get_value())
                    points.append(p)

        return points

    # medium rate points (hourly data)
    def medium_rate_points(self) -> List[Point]:

        # Collect points that summarize prior hour
        points = self.compute_hourly_Wh_points()
        points.extend(self.medium_rate_points_batteries())
        points.extend(self.compute_hourly_Wh_points_balkonkraftwerk())
        points.extend(self.compute_hourly_Wh_points_vzlogger())

        return points

    def medium_rate_points_batteries(self) -> List[Point]:

        # Collect points that summarize prior day
        points = self.compute_hourly_battery_Soc_points()
        points.extend(self.compute_hourly_battery_temperature_points())

        return points

    def compute_hourly_Wh_points(self) -> List[Point]:
        # Not using integral(interpolate:"linear") since it does not do what you
        # think it would mean. Without the "interoplation" arg, it still does
        # linear interpolation correctly.
        # https://github.com/influxdata/flux/issues/4782
        query = f"""
        from(bucket: "{self.cfg.influxdb_bucket_hr}")
            |> range(start: -1h, stop: 0h)
            |> filter(fn: (r) => r["source"] == "{self.cfg.source_tag}")
            |> filter(fn: (r) => r["_field"] == "P")
            |> integral(unit: 1h)
            |> keep(columns: ["_value", "line-idx", "measurement-type", "serial"])
            |> yield(name: "total")
        """
        result = self.influxdb_query_api.query(query=query)
        unreported_inverters = set(self.cfg.inverters.keys())
        points = []
        for table in result:
            for record in table.records:
                measurement_type = record['measurement-type']
                if measurement_type == "inverter":
                    serial = record['serial']
                    unreported_inverters.discard(serial)
                    p = Point(f"inverter-hourly-summary-{serial}")
                    p.tag("serial", serial)
                    self.cfg.apply_tags_to_inverter_point(p, serial)
                else:
                    idx = record['line-idx']
                    p = Point(f"{measurement_type}-hourly-summary-line{idx}")
                    p.tag("line-idx", idx)

                #p.time(ts, WritePrecision.S)
                p.tag("source", self.cfg.source_tag)
                p.tag("measurement-type", measurement_type)
                p.tag("interval", "1h")

                p.field("Wh", record.get_value())
                points.append(p)

        # If any inverters did not report in for the day, fill in a 0wh measurement
        for serial in unreported_inverters:
            p = Point(f"inverter-hourly-summary-{serial}")
            p.tag("serial", serial)
            self.cfg.apply_tags_to_inverter_point(p, serial)
            #p.time(ts, WritePrecision.S)
            p.tag("source", self.cfg.source_tag)
            p.tag("measurement-type", measurement_type)
            p.tag("interval", "1h")
            p.field("Wh", 0.0)
            points.append(p)

        return points

    def compute_hourly_battery_Soc_points(self) -> List[Point]:

        query = f"""
        from(bucket: "{self.cfg.influxdb_bucket_hr}")
            |> range(start: -1h, stop: now())
            |> filter(fn: (r) => r["source"] == "{self.cfg.source_tag}")
            |> filter(fn: (r) => r["measurement-type"] == "battery")
            |> filter(fn: (r) => r["_field"] == "percentFull")
            |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
            |> yield(name: "mean_soc")
            
        from(bucket: "{self.cfg.influxdb_bucket_hr}")
            |> range(start: -1h, stop: now())
            |> filter(fn: (r) => r["source"] == "{self.cfg.source_tag}")
            |> filter(fn: (r) => r["measurement-type"] == "battery")
            |> filter(fn: (r) => r["_field"] == "percentFull")
            |> aggregateWindow(every: 1h, fn: max, createEmpty: false)
            |> yield(name: "max_soc")
            
        from(bucket: "{self.cfg.influxdb_bucket_hr}")
            |> range(start: -1h, stop: now())
            |> filter(fn: (r) => r["source"] == "{self.cfg.source_tag}")
            |> filter(fn: (r) => r["measurement-type"] == "battery")
            |> filter(fn: (r) => r["_field"] == "percentFull")
            |> aggregateWindow(every: 1h, fn: min, createEmpty: false)
            |> yield(name: "min_soc")
        """
        result = self.influxdb_query_api.query(query=query)
        points = []
        for table in result:
            for record in table.records:
                measurement_type = record['measurement-type']
                if measurement_type == "battery":
                    serial = record['serial']
                    p = Point(f"battery-hourly-summary-{serial}")
                    p.tag("serial", serial)
                    #p.time(ts, WritePrecision.S)
                    p.tag("source", self.cfg.source_tag)
                    p.tag("measurement-type", measurement_type)
                    p.tag("interval", "1h")

                    p.field(record["result"], record.get_value())
                    points.append(p)

        return points

    def compute_hourly_battery_temperature_points(self) -> List[Point]:

        query = f"""
        from(bucket: "{self.cfg.influxdb_bucket_hr}")
            |> range(start: -1h, stop: now())
            |> filter(fn: (r) => r["source"] == "{self.cfg.source_tag}")
            |> filter(fn: (r) => r["measurement-type"] == "battery")
            |> filter(fn: (r) => r["_field"] == "temperature")
            |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
            |> yield(name: "mean_temperature")
        """
        result = self.influxdb_query_api.query(query=query)
        points = []
        for table in result:
            for record in table.records:
                measurement_type = record['measurement-type']
                if measurement_type == "battery":
                    serial = record['serial']
                    p = Point(f"battery-hourly-summary-{serial}")
                    p.tag("serial", serial)
                    #p.time(ts, WritePrecision.S)
                    p.tag("source", self.cfg.source_tag)
                    p.tag("measurement-type", measurement_type)
                    p.tag("interval", "1h")

                    p.field(record["result"], record.get_value())
                    points.append(p)

        return points

    def compute_daily_Wh_points_balkonkraftwerk(self) -> List[Point]:
        query = """
        from(bucket: "balkonkraftwerk")
            |> range(start: -24h, stop: 0h)
            |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")
            |> filter(fn: (r) => r["topic"] == "inverter/HM-800/ch0/YieldTotal")
            |> aggregateWindow(every: 24h, fn: last)
            |> spread()
            |> map(fn: (r) => ({r with _value: float(v: r._value) * 1000.00}))
        """
        result = self.influxdb_query_api.query(query=query)
        points = []
        for table in result:
            for record in table.records:
                measurement_type = record['_measurement']
                if measurement_type == "mqtt_consumer":
                    serial = record['host']
                    p = Point(f"balkonkraftwerk-daily-summary-{serial}")
                    p.tag("serial", serial)
                    #p.time(ts, WritePrecision.S)
                    p.tag("source", "balkonkraftwerk")
                    p.tag("measurement-type", measurement_type)
                    p.tag("interval", "24h")

                    p.field("Wh", record.get_value())
                    points.append(p)

        return points

    def compute_hourly_Wh_points_balkonkraftwerk(self) -> List[Point]:
        query = """
        from(bucket: "balkonkraftwerk")
            |> range(start: -2h, stop: now())
            |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")
            |> filter(fn: (r) => r["topic"] == "inverter/HM-800/ch0/YieldTotal")
            |> aggregateWindow(every: 1h, fn: last)
            |> spread()
            |> map(fn: (r) => ({r with _value: float(v: r._value) * 1000.00}))
        """
        result = self.influxdb_query_api.query(query=query)
        points = []
        for table in result:
            for record in table.records:
                measurement_type = record['_measurement']
                if measurement_type == "mqtt_consumer":
                    serial = record['host']
                    p = Point(f"balkonkraftwerk-hourly-summary-{serial}")
                    p.tag("serial", serial)
                    #p.time(ts, WritePrecision.S)
                    p.tag("source", "balkonkraftwerk")
                    p.tag("measurement-type", measurement_type)
                    p.tag("interval", "1h")

                    p.field("Wh", record.get_value())
                    points.append(p)

        return points

    def compute_daily_Wh_points_vzlogger(self) -> List[Point]:
        query = """
        from(bucket: "vzlogger")
            |> range(start: -24h, stop: 0h)
            |> filter(fn: (r) => r["_measurement"] == "vz_measurement")
            |> filter(fn: (r) => r["meter"] == "hausstrom")
            |> filter(fn: (r) => r["measurement"] == "zaehlerstand" or r["measurement"] == "zaehlerstand_einspeisung")
            |> filter(fn: (r) => r["_field"] == "value")
            |> aggregateWindow(every: 24h, fn: last)
            |> spread()
        """
        result = self.influxdb_query_api.query(query=query)
        points = []
        for table in result:
            for record in table.records:
                measurement_type = record['measurement']
                serial = record['meter']
                p = Point(f"{measurement_type}-daily-summary-{serial}")
                p.tag("serial", serial)
                #p.time(ts, WritePrecision.S)
                p.tag("source", record['meter'])
                p.tag("measurement-type", measurement_type)
                p.tag("interval", "24h")

                p.field("Wh", record.get_value())
                points.append(p)

        return points

    def compute_hourly_Wh_points_vzlogger(self) -> List[Point]:
        query = """
        from(bucket: "vzlogger")
            |> range(start: -2h, stop: now())
            |> filter(fn: (r) => r["_measurement"] == "vz_measurement")
            |> filter(fn: (r) => r["meter"] == "hausstrom")
            |> filter(fn: (r) => r["measurement"] == "zaehlerstand" or r["measurement"] == "zaehlerstand_einspeisung")
            |> filter(fn: (r) => r["_field"] == "value")
            |> aggregateWindow(every: 1h, fn: last)
            |> spread()
        """
        result = self.influxdb_query_api.query(query=query)
        points = []
        for table in result:
            for record in table.records:
                measurement_type = record['measurement']
                serial = record['meter']
                p = Point(f"{measurement_type}-hourly-summary-{serial}")
                p.tag("serial", serial)
                #p.time(ts, WritePrecision.S)
                p.tag("source", record['meter'])
                p.tag("measurement-type", measurement_type)
                p.tag("interval", "1h")

                p.field("Wh", record.get_value())
                points.append(p)

        return points

