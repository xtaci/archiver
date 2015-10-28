package main

import (
	log "github.com/gonet2/libs/nsq-logger"
	_ "github.com/gonet2/libs/statsd-pprof"
)

func main() {
	log.SetPrefix(SERVICE)
	arch := &Archiver{}
	arch.init()
	<-arch.stop
}
