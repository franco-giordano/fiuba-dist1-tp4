FROM rabbitmq-python-base:0.0.1
RUN apk update
RUN apk add docker
COPY common /common
COPY master /
CMD ["python3", "./main.py"]