FROM golang:1.5
MAINTAINER xtaci <daniel820313@gmail.com>
ENV GOBIN /go/bin
COPY .godeps /go/.godeps
COPY src /go/src
COPY scripts /go/scripts
WORKDIR /go
ENV GOPATH /go:/go/.godeps
RUN go install archiver
RUN go install replay
RUN rm -rf pkg src .godeps
RUN mkdir /data
VOLUME /data
