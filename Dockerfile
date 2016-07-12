FROM golang:latest
MAINTAINER xtaci <daniel820313@gmail.com>
ENV GOBIN /go/bin
COPY src /go/src
COPY scripts /go/scripts
WORKDIR /go
RUN go install archiver
RUN go install replay
RUN rm -rf pkg src
RUN mkdir /data
VOLUME /data
