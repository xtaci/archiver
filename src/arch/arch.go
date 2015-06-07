package ipc

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
	DEFAULT_NSQLOOKUPD   = "127.0.0.1:4160"
	ENV_NSQLOOKUPD       = "NSQLOOKUPD_HOST"
	TOPIC                = "REDOLOG"
	CHANNEL              = "ARCH"
	SERVICE              = "[ARCH]"
	REDO_TIME_FORMAT     = "REDO-2006-01-02T15:04:05.RDO"
	REDO_ROTATE_INTERVAL = 24 * time.Hour
	BOLTDB_BUCKET        = "REDOLOG"
)

type Archiver struct {
	pending chan []byte
}

func (arch *Archiver) init() {
	arch.pending = make(chan []byte)
	cfg := nsq.NewConfig()
	consumer, err := nsq.NewConsumer(TOPIC, CHANNEL, cfg)
	if err != nil {
		log.Critical(err)
		os.Exit(-1)
	}

	// message process
	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
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
	db, err := bolt.Open(time.Now().Format(REDO_TIME_FORMAT), 0600, nil)
	if err != nil {
		log.Critical(err)
		os.Exit(-1)
	}
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
			db, err = bolt.Open(time.Now().Format(REDO_TIME_FORMAT), 0600, nil)
			if err != nil {
				log.Critical(err)
				os.Exit(-1)
			}
			timer = time.After(REDO_ROTATE_INTERVAL)
		}
	}
}
