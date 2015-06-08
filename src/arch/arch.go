package main

import (
	"fmt"
	log "github.com/GameGophers/nsq-logger"
	nsq "github.com/bitly/go-nsq"
	"github.com/boltdb/bolt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"os"
	"strings"
	"time"
)

const (
	DEFAULT_NSQLOOKUPD   = "http://127.0.0.1:4161"
	ENV_NSQLOOKUPD       = "NSQLOOKUPD_HOST"
	TOPIC                = "REDOLOG"
	CHANNEL              = "ARCH"
	SERVICE              = "[ARCH]"
	REDO_TIME_FORMAT     = "REDO-2006-01-02T15:04:05.RDO"
	REDO_ROTATE_INTERVAL = 24 * time.Hour
	BOLTDB_BUCKET        = "REDOLOG"
	DATA_DIRECTORY       = "/data"
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
	timer := time.After(REDO_ROTATE_INTERVAL)
	db := arch.new_redolog()
	for {
		select {
		case msg := <-arch.pending:
			var record map[string]interface{}
			err := msgpack.Unmarshal(msg, &record)
			if err != nil {
				log.Error(err)
				continue
			}

			db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(BOLTDB_BUCKET))
				err := b.Put([]byte(fmt.Sprint(record["TS"])), msg)
				return err
			})
		case <-timer:
			db.Close()
			// rotate redolog
			db = arch.new_redolog()
			timer = time.After(REDO_ROTATE_INTERVAL)
		}
	}
}

func (arch *Archiver) new_redolog() *bolt.DB {
	file := DATA_DIRECTORY + "/" + time.Now().Format(REDO_TIME_FORMAT)
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
		}
		return nil
	})
	return db
}
