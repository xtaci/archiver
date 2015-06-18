package main

import (
	log "github.com/GameGophers/libs/nsq-logger"
	_ "github.com/GameGophers/libs/statsd-pprof"
)

func main() {
	log.SetPrefix(SERVICE)
	arch := &Archiver{}
	arch.init()
	<-arch.stop
}
