package main

import (
	"encoding/binary"
	"encoding/json"
	"github.com/boltdb/bolt"
	"github.com/yuin/gopher-lua"
	"gopkg.in/mgo.v2/bson"
	"log"
)

// a data change
type Change struct {
	Collection string // collection
	Field      string // field "a.b.c.d"
	Doc        interface{}
}

// a redo record represents complete transaction
type RedoRecord struct {
	Id      int      // records index
	API     string   // the api name
	UID     int32    // userid
	TS      uint64   // timestamp should get from snowflake
	Changes []Change // changes
}

func (t *ToolBox) builtin_get(L *lua.LState) int {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.([]rec); ok {
		if L.GetTop() == 2 {
			idx := L.CheckInt(2) - 1
			if idx >= 0 && idx < len(v) {
				elem := v[idx]
				L.Push(t.read(idx, elem.db_idx, elem.key))
				return 1
			} else {
				L.ArgError(1, "index out of range")
				return 0
			}
		}
	}
	return 0
}
func (t *ToolBox) builtin_length(L *lua.LState) int {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.([]rec); ok {
		L.Push(lua.LNumber(len(v)))
		return 1
	}
	return 0
}

func (t *ToolBox) read(idx int, db_idx int, key uint64) lua.LString {
	r := &RedoRecord{}
	t.dbs[db_idx].View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET))
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(key))
		bin := b.Get(k)
		if bin == nil {
			log.Println("no such record")
			return nil
		}
		err := bson.Unmarshal(bin, r)
		if err != nil {
			log.Println(err)
			return nil
		}
		return nil
	})
	r.Id = idx
	if bin, err := json.MarshalIndent(r, "", "\t"); err == nil {
		return lua.LString(bin)
	} else {
		log.Println(err)
		return ""
	}
}
