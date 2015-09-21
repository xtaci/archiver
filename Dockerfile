FROM golang:1.5
MAINTAINER xtaci <daniel820313@gmail.com>
ENV GOBIN /go/bin
COPY . /go
WORKDIR /go
RUN mkdir /data
ENV GOPATH /go:/go/.godeps
RUN go install arch
RUN rm -rf pkg src .godeps
ENTRYPOINT /go/bin/arch
VOLUME /data
