FROM ubuntu:18.04

# Install golang
RUN apt update && apt install python3 python3-pip -y
RUN pip3 install pika