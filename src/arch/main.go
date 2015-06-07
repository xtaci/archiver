package main

import (
	log "github.com/GameGophers/nsq-logger"
	_ "github.com/GameGophers/statsd-pprof"
)

func main() {
	log.SetPrefix(SERVICE)
	arch := &Archiver{}
	arch.init()
	<-arch.stop
}
