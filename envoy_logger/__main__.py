import logging
import argparse
import time
from tzlocal import get_localzone


from requests.exceptions import RequestException

from envoy_logger import enphaseenergy
from envoy_logger.sampling_loop import SamplingLoop
from envoy_logger.cfg import load_cfg

from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s]: %(message)s"
)

parser = argparse.ArgumentParser()
parser.add_argument("cfg_path")
args = parser.parse_args()

cfg = load_cfg(args.cfg_path)

while True:
    # Loop forever so that if an exception occurs, logger will restart
    try:
        envoy_token = enphaseenergy.get_token(
            cfg.enphase_email,
            cfg.enphase_password,
            cfg.envoy_serial
        )

        S = SamplingLoop(envoy_token, cfg)

        if cfg.calc_hourly_data:
            tz = get_localzone()
            scheduler_hourly = BackgroundScheduler({'apscheduler.timezone': tz})
            scheduler_hourly.add_job(S.write_to_influxdb_hourly, 'cron', hour="*", minute=0, second=0)
            scheduler_hourly.start()

        if cfg.calc_daily_data:
            tz = get_localzone()
            scheduler_hourly = BackgroundScheduler({'apscheduler.timezone': tz})
            scheduler_daily = BackgroundScheduler()
            scheduler_daily.add_job(S.write_to_influxdb_daily, 'cron', hour=0)
            scheduler_daily.start()

        S.run()
    except Exception as e:
        logging.error("%s: %s", str(type(e)), e)

        if cfg.calc_hourly_data:
            try:
                scheduler_hourly.shutdown(wait=False)
            except Exception as e:
                logging.warning("scheduler_hourly could not be shutdown correctly")
                scheduler_hourly = None

        if cfg.calc_daily_data:
            try:
                scheduler_daily.shutdown(wait=False)
            except Exception as e:
                logging.warning("scheduler_daily could not be shutdown correctly")
                scheduler_daily = None

        # sleep 5 minutes to recover
        time.sleep(300)
        logging.info("Restarting data logger")
