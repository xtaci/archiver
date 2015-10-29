package main

import (
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/vmihailenco/msgpack.v2"
	"time"
)

// a data change
type Change struct {
	Collection string // collection
	Field      string // field "a.b.c.d"
	Doc        []byte // msgpack serialized data
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

func (t *ToolBox) cmd_p() {
	tk := t.next()
	if tk.typ == TK_NUM { // p with param
		if tk.num >= len(t.dbs) {
			fmt.Println("no such file", tk.num)
			return
		}
		// stats
		t.dbs[tk.num].View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(BOLTDB_BUCKET))
			fmt.Printf("%#v\n", b.Stats())
			return nil
		})

		// users
		users := make(map[int32]int)
		t.dbs[tk.num].View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(BOLTDB_BUCKET))
			c := b.Cursor()
			brief := &Brief{}
			for k, v := c.First(); k != nil; k, v = c.Next() {
				err := msgpack.Unmarshal(v, brief)
				if err != nil {
					fmt.Println("data corrupted, record-id:", k)
					continue
				}
				users[brief.UID]++
			}
			return nil
		})
		fmt.Println("users of this db:")
		for userid, count := range users {
			fmt.Println("id:", userid, "count:", count)
		}
	} else { // only p
		for k, v := range t.dbs {
			fmt.Printf("%v -- %v\n", k, v)
		}
	}
}

func (t *ToolBox) cmd_help() {
	fmt.Println(help)
}

func (t *ToolBox) cmd_clear() {
	t.fileid = -1
	t.userid = -1
	t.duration_set = false
}

func (t *ToolBox) cmd_show() {
	if t.fileid == -1 {
		fmt.Println("bind first")
		return
	}
	recid_tk := t.match(TK_NUM)
	t.dbs[t.fileid].View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET))
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(recid_tk.num))
		bin := b.Get(key)
		if bin == nil {
			fmt.Println("no such record")
			return nil
		}
		r := &RedoRecord{}
		err := msgpack.Unmarshal(bin, r)
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
			raw := make(map[string]interface{})
			err := bson.Unmarshal(r.Changes[k].Doc, &raw)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("Doc %#v\n", raw)
		}
		return nil
	})
}

func (t *ToolBox) cmd_bind() {
	fileid_tk := t.match(TK_NUM)
	userid_tk := t.match(TK_NUM)
	if fileid_tk.num < len(t.dbs) {
		t.fileid = fileid_tk.num
		t.userid = userid_tk.num
		return
	}
	fmt.Println("no such file", fileid_tk.num)
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
	if t.fileid == -1 {
		fmt.Println("bind first")
		return
	}

	var ms_a, ms_b int64
	if t.duration_set {
		ms_a, ms_b = t.to_ms()
	}

	// count
	count := 0
	t.dbs[t.fileid].View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET))
		c := b.Cursor()
		brief := &Brief{}
		for k, v := c.First(); k != nil; k, v = c.Next() {
			err := msgpack.Unmarshal(v, brief)
			if err != nil {
				fmt.Println("data corrupted, record-id:", k)
				continue
			}
			if brief.UID == int32(t.userid) {
				if !t.duration_set {
					count++
				} else { // parse snowflake-id
					ms := int64(brief.TS >> 22)
					if ms >= ms_a && ms <= ms_b {
						count++
					}
				}
			}
		}
		return nil
	})

	fmt.Printf("total:%v\n", count)
}

func (t *ToolBox) cmd_ls() {
	if t.fileid == -1 {
		fmt.Println("bind first")
		return
	}
	var ms_a, ms_b int64
	if t.duration_set {
		ms_a, ms_b = t.to_ms()
	}
	t.dbs[t.fileid].View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET))
		c := b.Cursor()
		r := &Brief{}
		for k, v := c.First(); k != nil; k, v = c.Next() {
			err := msgpack.Unmarshal(v, r)
			if err != nil {
				fmt.Println("data corrupted, record-id:", k)
				continue
			}
			key := binary.BigEndian.Uint64(k)
			if r.UID == int32(t.userid) {
				if !t.duration_set {
					fmt.Printf("%v->%#v\n", key, r)
				} else { // parse snowflake-id
					ms := int64(r.TS >> 22)
					if ms >= ms_a && ms <= ms_b {
						fmt.Printf("%v->%#v\n", key, r)
					}
				}
			}
		}
		return nil
	})
}

func (t *ToolBox) cmd_replay() {
	if t.fileid == -1 {
		fmt.Println("bind first")
		return
	}
	mgo_tk := t.match(TK_STRING)
	sess, err := mgo.Dial(mgo_tk.literal)
	if err != nil {
		fmt.Println(err)
		return
	}
	var ms_a, ms_b int64
	if t.duration_set {
		ms_a, ms_b = t.to_ms()
	}
	t.dbs[t.fileid].View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET))
		c := b.Cursor()
		r := &RedoRecord{}
		for k, v := c.First(); k != nil; k, v = c.Next() {
			err := msgpack.Unmarshal(v, r)
			if err != nil {
				fmt.Println("data corrupted, record-id:", k)
				continue
			}
			if r.UID == int32(t.userid) {
				if !t.duration_set {
					do_update(k, r, sess)
				} else { // parse snowflake-id
					ms := int64(r.TS >> 22)
					if ms >= ms_a && ms <= ms_b {
						do_update(k, r, sess)
					}
				}
			}
		}
		return nil
	})
}

func (t *ToolBox) to_ms() (int64, int64) {
	return t.duration_a.UnixNano() / int64(time.Millisecond), t.duration_b.UnixNano() / int64(time.Millisecond)
}

func do_update(k []byte, r *RedoRecord, sess *mgo.Session) {
	fmt.Println("UPDATING:", binary.BigEndian.Uint64(k))
	mdb := sess.DB("")
	for k := range r.Changes {
		fmt.Printf("Doing Update On Collection:%v Field:%v\n", r.Changes[k].Collection, r.Changes[k].Field)
		raw := make(map[string]interface{})
		err := bson.Unmarshal(r.Changes[k].Doc, &raw)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if r.Changes[k].Field != "" {
			_, err = mdb.C(r.Changes[k].Collection).Upsert(bson.M{"userid": r.UID}, bson.M{"$set": bson.M{r.Changes[k].Field: raw}})
		} else {
			_, err = mdb.C(r.Changes[k].Collection).Upsert(bson.M{"userid": r.UID}, raw)
		}
		if err != nil {
			fmt.Println(err)
			continue
		}
	}
}
