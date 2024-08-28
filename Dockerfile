# syntax=docker/dockerfile:1
FROM ubuntu:22.04

WORKDIR /home/yrd

RUN apt-get -y update && apt-get -y upgrade
RUN apt-get install -y wget tar
RUN wget https://go.dev/dl/go1.23.0.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.23.0.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin
RUN go version
RUN apt-get -y install gcc
RUN apt-get -y install gdb
RUN apt-get -y install build-essential

CMD ["sleep", "infinity"]
