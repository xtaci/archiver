package main

import (
	"encoding/binary"
	nsq "github.com/bitly/go-nsq"
	"github.com/boltdb/bolt"
	log "github.com/gonet2/libs/nsq-logger"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	DEFAULT_NSQLOOKUPD   = "http://172.17.42.1:4161"
	ENV_NSQLOOKUPD       = "NSQLOOKUPD_HOST"
	TOPIC                = "REDOLOG"
	CHANNEL              = "ARCH"
	SERVICE              = "[ARCH]"
	REDO_TIME_FORMAT     = "REDO-2006-01-02T15:04:05.RDO"
	REDO_ROTATE_INTERVAL = 24 * time.Hour
	BOLTDB_BUCKET        = "REDOLOG"
	DATA_DIRECTORY       = "/data/"
)

type Archiver struct {
	pending chan []byte
	stop    chan bool
}

func (arch *Archiver) init() {
	arch.pending = make(chan []byte)
	arch.stop = make(chan bool)
	cfg := nsq.NewConfig()
	consumer, err := nsq.NewConsumer(TOPIC, CHANNEL, cfg)
	if err != nil {
		log.Critical(err)
		os.Exit(-1)
	}

	// message process
	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		arch.pending <- msg.Body
		return nil
	}))

	// read environtment variable
	addresses := []string{DEFAULT_NSQLOOKUPD}
	if env := os.Getenv(ENV_NSQLOOKUPD); env != "" {
		addresses = strings.Split(env, ";")
	}

	// connect to nsqlookupd
	log.Trace("connect to nsqlookupds ip:", addresses)
	if err := consumer.ConnectToNSQLookupds(addresses); err != nil {
		log.Critical(err)
		return
	}
	log.Info("nsqlookupd connected")

	go arch.archive_task()
}

func (arch *Archiver) archive_task() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	timer := time.After(REDO_ROTATE_INTERVAL)
	db := arch.new_redolog()
	key := make([]byte, 8)
	for {
		select {
		case msg := <-arch.pending:
			db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(BOLTDB_BUCKET))
				id, err := b.NextSequence()
				if err != nil {
					log.Critical(err)
					return err
				}
				binary.BigEndian.PutUint64(key, uint64(id))
				if err = b.Put(key, msg); err != nil {
					log.Critical(err)
					return err
				}
				return nil
			})
		case <-timer:
			db.Close()
			// rotate redolog
			db = arch.new_redolog()
			timer = time.After(REDO_ROTATE_INTERVAL)
		case <-sig:
			db.Close()
			log.Info("SIGTERM")
			os.Exit(0)
		}
	}
}

func (arch *Archiver) new_redolog() *bolt.DB {
	file := DATA_DIRECTORY + time.Now().Format(REDO_TIME_FORMAT)
	log.Info(file)
	db, err := bolt.Open(file, 0600, nil)
	if err != nil {
		log.Critical(err)
		os.Exit(-1)
	}
	// create bulket
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(BOLTDB_BUCKET))
		if err != nil {
			log.Criticalf("create bucket: %s", err)
			return err
		}
		return nil
	})
	return db
}
