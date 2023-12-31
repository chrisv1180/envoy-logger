FROM python:3.11-slim-bookworm

ADD https://raw.githubusercontent.com/chrisv1180/envoy-logger/main/launcher.sh /
RUN chmod +x /launcher.sh

# Install Stuff
RUN apt update
RUN apt -y install git

ENV ENVOY_LOGGER_CFG_PATH=/etc/envoy_logger/cfg.yaml

# entrypoint
CMD /launcher.sh
