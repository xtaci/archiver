FROM golang:1.5
MAINTAINER xtaci <daniel820313@gmail.com>
ENV GOBIN /go/bin
COPY . /go
WORKDIR /go
ENV GOPATH /go:/go/.godeps
RUN go install archiver
RUN rm -rf pkg src .godeps
ENTRYPOINT /go/bin/archiver
RUN mkdir /data
VOLUME /data
