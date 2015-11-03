package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"gopkg.in/mgo.v2/bson"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
	"unicode"
)

const (
	TK_UNDEFINED = iota
	TK_LS
	TK_CLEAR
	TK_HELP
	TK_REPLAY
	TK_SHOW
	TK_NUM
	TK_STRING
	TK_SUM
	TK_USER
	TK_DURATION
	TK_EOF
)

var cmds = map[string]int{
	"help":  TK_HELP,
	"clear": TK_CLEAR,

	"user":     TK_USER,
	"sum":      TK_SUM,
	"duration": TK_DURATION,
	"ls":       TK_LS,
	"replay":   TK_REPLAY,
	"show":     TK_SHOW,
}

type token struct {
	typ     int
	literal string
	num     int
}

// set ts=8
var help = `REDO Replay Tool
Commands:

> help					-- print this text
> clear 				-- clear all bindings
> ls					-- list all elements
> sum					-- count all elements
> show 33				-- show detailed records with id 33
> replay "mongodb://172.17.42.1/mydb"	-- replay all changes

Bind Operations to user:
> user 1234		-- all operations below are binded to a user#1234
> sum 			-- print number of records of the user
> ls 			-- list all elements of the user
> replay "mongodb://172.17.42.1/mydb"	-- replay all changes of the user

Bind operations to duration:
> duration "2015-10-28T14:53:27"  "2015-10-29T14:53:27"
(all operations below are binded to this duration)
> sum		-- print number of records in this duration
> ls 		-- show all elements in this duration
> replay "mongodb://172.17.42.1/mydb"	-- replay all changes in this duration
`

type rec struct {
	db_idx int    // file
	key    uint64 // key of file
	userid int32
	ts     uint64
}

type ToolBox struct {
	dbs          []*bolt.DB // all opened boltdb
	userid       int        // current selected userid
	recs         []rec
	duration_a   time.Time
	duration_b   time.Time
	duration_set bool
	mgo_url      string
	cmd_reader   *bytes.Buffer // cmds
}

type file_sort []string

func (a file_sort) Len() int      { return len(a) }
func (a file_sort) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a file_sort) Less(i, j int) bool {
	layout := "REDO-2006-01-02T15:04:05.RDO"
	tm_a, _ := time.Parse(layout, a[i])
	tm_b, _ := time.Parse(layout, a[j])
	return tm_a.Unix() < tm_b.Unix()
}

func (t *ToolBox) init(dir string) {
	t.cmd_clear()
	files, err := filepath.Glob(dir + "/*.RDO")
	if err != nil {
		log.Println(err)
		os.Exit(-1)
	}
	// sort by creation time
	sort.Sort(file_sort(files))

	for _, file := range files {
		db, err := bolt.Open(file, 0600, &bolt.Options{Timeout: 2 * time.Second, ReadOnly: true})
		if err != nil {
			log.Println(err)
			continue
		}
		t.dbs = append(t.dbs, db)
	}

	// reindex all keys
	for i := range t.dbs {
		t.dbs[i].View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(BOLTDB_BUCKET))
			c := b.Cursor()
			var meta struct {
				UID int32
				TS  uint64
			}
			for k, v := c.First(); k != nil; k, v = c.Next() {
				err := bson.Unmarshal(v, &meta)
				if err != nil {
					log.Println(err)
					continue
				}
				t.recs = append(t.recs, rec{i, binary.BigEndian.Uint64(k), meta.UID, meta.TS})
			}
			return nil
		})
	}
}

//////////////////////////////////////////
// parser
func (t *ToolBox) next() *token {
	var r rune
	var err error
	for {
		r, _, err = t.cmd_reader.ReadRune()
		if err == io.EOF {
			return &token{typ: TK_EOF}
		} else if unicode.IsSpace(r) {
			continue
		}
		break
	}

	if unicode.IsLetter(r) {
		var runes []rune
		for {
			runes = append(runes, r)
			r, _, err = t.cmd_reader.ReadRune()
			if err == io.EOF {
				break
			} else if unicode.IsLetter(r) {
				continue
			} else {
				t.cmd_reader.UnreadRune()
				break
			}
		}

		t := &token{}
		t.literal = string(runes)
		t.typ = cmds[t.literal]
		return t
	} else if r == '"' { // quoted string
		var runes []rune
		for {
			r, _, err = t.cmd_reader.ReadRune()
			if err == io.EOF {
				break
			} else if r != '"' { // read until '"'
				runes = append(runes, r)
				continue
			} else {
				break
			}
		}
		t := &token{}
		t.literal = string(runes)
		t.typ = TK_STRING
		return t
	} else if unicode.IsDigit(r) {
		var runes []rune
		for {
			runes = append(runes, r)
			r, _, err = t.cmd_reader.ReadRune()
			if err == io.EOF {
				break
			} else if unicode.IsDigit(r) {
				continue
			} else {
				t.cmd_reader.UnreadRune()
				break
			}
		}

		t := &token{}
		t.num, _ = strconv.Atoi(string(runes))
		t.typ = TK_NUM
		return t
	}
	return &token{}
}

func (t *ToolBox) match(typ int) *token {
	tk := t.next()
	if tk.typ != typ {
		panic("syntax error")
	}
	return tk
}

func (t *ToolBox) parse_exec(cmd string) {
	defer func() {
		if x := recover(); x != nil {
			fmt.Println(x, cmd)
		}
	}()
	t.cmd_reader = bytes.NewBufferString(cmd)
	tk := t.next()
	switch tk.typ {
	case TK_HELP:
		t.cmd_help()
	case TK_CLEAR:
		t.cmd_clear()
	case TK_DURATION:
		t.cmd_duration()
	case TK_USER:
		t.cmd_user()
	case TK_SUM:
		t.cmd_sum()
	case TK_LS:
		t.cmd_ls()
	case TK_REPLAY:
		t.cmd_replay()
	case TK_SHOW:
		t.cmd_show()
	default:
		fmt.Println("unkown command:", cmd)
	}
}
