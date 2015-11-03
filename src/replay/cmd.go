package main

import (
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

// a data change
type Change struct {
	Collection string // collection
	Field      string // field "a.b.c.d"
	Doc        interface{}
}

// a redo record represents complete transaction
type RedoRecord struct {
	API     string   // the api name
	UID     int32    // userid
	TS      uint64   // timestamp should get from snowflake
	Changes []Change // changes
}

// a redo record represents complete transaction
type Brief struct {
	API string // the api name
	UID int32  // userid
	TS  uint64 // timestamp should get from snowflake
}

const (
	BOLTDB_BUCKET = "REDOLOG"
	LAYOUT        = "2006-01-02T15:04:05"
)

func (t *ToolBox) cmd_help() {
	fmt.Println(help)
}

func (t *ToolBox) cmd_clear() {
	t.userid = -1
	t.duration_set = false
}

func (t *ToolBox) cmd_show() {
	recid_tk := t.match(TK_NUM)
	rec := t.recs[recid_tk.num]
	t.dbs[rec.db_idx].View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET))
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(rec.key))
		bin := b.Get(key)
		if bin == nil {
			fmt.Println("no such record")
			return nil
		}
		r := &RedoRecord{}
		err := bson.Unmarshal(bin, r)
		if err != nil {
			fmt.Println("data corrupted")
			return nil
		}

		fmt.Println("UserId:", r.UID)
		fmt.Println("API:", r.API)
		ts := int64(r.TS >> 22)
		fmt.Println("CreatedAt:", time.Unix(ts/1000, 0))
		for k := range r.Changes {
			fmt.Printf("Change #%v Collection:%v Field:%v\n", k, r.Changes[k].Collection, r.Changes[k].Field)
			fmt.Printf("\tDoc:%v\n", r.Changes[k].Doc)
		}
		return nil
	})
}

func (t *ToolBox) cmd_user() {
	tk := t.match(TK_NUM)
	t.userid = tk.num
}

func (t *ToolBox) cmd_duration() {
	tk_a := t.match(TK_STRING)
	tk_b := t.match(TK_STRING)

	tm_a, err := time.Parse(LAYOUT, tk_a.literal)
	if err != nil {
		fmt.Println(err)
		return
	}

	tm_b, err := time.Parse(LAYOUT, tk_b.literal)
	if err != nil {
		fmt.Println(err)
		return
	}

	t.duration_a = tm_a
	t.duration_b = tm_b
	t.duration_set = true
}

func (t *ToolBox) cmd_sum() {
	// count
	count := 0
	t.binded(func(i int) {
		count++
	})
	fmt.Printf("total:%v\n", count)
}

func (t *ToolBox) cmd_ls() {
	t.binded(func(i int) {
		fmt.Printf("REC#%v userid%v\n", i, t.recs[i].userid)
	})
}

func (t *ToolBox) binded(f func(i int)) {
	var ms_a, ms_b int64
	if t.duration_set {
		ms_a, ms_b = t.to_ms()
	}

	for k := range t.recs {
		ok := true
		if t.duration_set {
			ms := int64(t.recs[k].ts >> 22)
			if ms < ms_a || ms > ms_b {
				ok = false
			}
		}
		if t.userid > 0 && t.recs[k].userid != int32(t.userid) {
			ok = false
		}
		if ok {
			f(k)
		}
	}
}

func (t *ToolBox) cmd_replay() {
	mgo_tk := t.match(TK_STRING)
	sess, err := mgo.Dial(mgo_tk.literal)
	if err != nil {
		fmt.Println(err)
		return
	}

	key := make([]byte, 8)
	t.binded(func(i int) {
		rec := &t.recs[i]
		t.dbs[rec.db_idx].View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(BOLTDB_BUCKET))
			binary.BigEndian.PutUint64(key, uint64(rec.key))
			bin := b.Get(key)
			if bin == nil {
				fmt.Println("no such record")
				return nil
			}
			r := &RedoRecord{}
			err := bson.Unmarshal(bin, r)
			if err != nil {
				fmt.Println("data corrupted")
				return nil
			}

			do_update(key, r, sess)
			return nil
		})
	})

	sess.Close()
}

func (t *ToolBox) to_ms() (int64, int64) {
	return t.duration_a.UnixNano() / int64(time.Millisecond), t.duration_b.UnixNano() / int64(time.Millisecond)
}

func do_update(k []byte, r *RedoRecord, sess *mgo.Session) {
	fmt.Println("UPDATING:", binary.BigEndian.Uint64(k))
	mdb := sess.DB("")
	for k := range r.Changes {
		fmt.Printf("Doing Update On Collection:%v Field:%v\n", r.Changes[k].Collection, r.Changes[k].Field)
		var err error
		if r.Changes[k].Field != "" {
			_, err = mdb.C(r.Changes[k].Collection).Upsert(bson.M{"userid": r.UID}, bson.M{"$set": bson.M{r.Changes[k].Field: r.Changes[k].Doc}})
		} else {
			_, err = mdb.C(r.Changes[k].Collection).Upsert(bson.M{"userid": r.UID}, r.Changes[k].Doc)
		}
		if err != nil {
			fmt.Println(err)
			continue
		}
	}
}
