package main

import (
	"fmt"
	"github.com/boltdb/bolt"
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
	for k, v := range t.dbs {
		fmt.Printf("%v -- %v\n", k, v)
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

func (t *ToolBox) cmd_u() {
	fileid_tk := t.match(TK_NUM)
	if fileid_tk.num >= len(t.dbs) {
		fmt.Println("no such file", fileid_tk.num)
		return
	}
	users := make(map[int32]bool)
	t.dbs[fileid_tk.num].View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET))
		c := b.Cursor()
		brief := &Brief{}
		for k, v := c.First(); k != nil; k, v = c.Next() {
			err := msgpack.Unmarshal(v, brief)
			if err != nil {
				fmt.Println("data corrupted, record-id:", k)
				continue
			}
			users[brief.UID] = true
		}
		return nil
	})

	fmt.Println("users of this db:")
	for userid := range users {
		fmt.Println(userid)
	}
}

func (t *ToolBox) cmd_s() {
	fileid_tk := t.match(TK_NUM)
	if fileid_tk.num >= len(t.dbs) {
		fmt.Println("no such file", fileid_tk.num)
		return
	}

	t.dbs[fileid_tk.num].View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET))
		fmt.Printf("%#v\n", b.Stats())
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

func (t *ToolBox) cmd_count() {
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
		r := &RedoRecord{}
		for k, v := c.First(); k != nil; k, v = c.Next() {
			err := msgpack.Unmarshal(v, r)
			if err != nil {
				fmt.Println("data corrupted, record-id:", k)
				continue
			}
			if r.UID == int32(t.userid) {
				if !t.duration_set {
					fmt.Printf("%#v\n", r)
				} else { // parse snowflake-id
					ms := int64(r.TS >> 22)
					if ms >= ms_a && ms <= ms_b {
						fmt.Printf("%#v\n", r)
					}
				}
			}
		}
		return nil
	})
}

func (t *ToolBox) cmd_replay() {
	fmt.Println("TODO: implement replay")
}

func (t *ToolBox) to_ms() (int64, int64) {
	return t.duration_a.UnixNano() / int64(time.Millisecond), t.duration_b.UnixNano() / int64(time.Millisecond)
}
