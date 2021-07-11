FROM rabbitmq:3.7.14-management
RUN apt-get update
RUN apt-get install -y curl