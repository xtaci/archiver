package main

import (
	"bytes"
	"fmt"
	"github.com/boltdb/bolt"
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
	TK_P
	TK_S
	TK_U
	TK_LS
	TK_CLEAR
	TK_HELP
	TK_REPLAY
	TK_SHOW
	TK_NUM
	TK_STRING
	TK_COUNT
	TK_BIND
	TK_DURATION
	TK_EOF
)

var cmds = map[string]int{
	"p":     TK_P,
	"help":  TK_HELP,
	"clear": TK_CLEAR,

	"u": TK_U,
	"s": TK_S,

	"bind":     TK_BIND,
	"sum":      TK_COUNT,
	"duration": TK_DURATION,
	"ls":       TK_LS,
	"replay":   TK_REPLAY,
}

type token struct {
	typ     int
	literal string
	num     int
}

// set ts=8
var help = `REDO Replay Tool
Commands:

> p 		-- list all database files(sorted by time):
> help		-- print this text
> clear 	-- clear all bindings

Global operations to file:
> u1		-- print all users of file#1
> s2		-- print summary of file#2

Bind Operations to user:
> bind 1 1234		-- all operations below are binded to a file#1 & user#1234
> sum 			-- print number of records of the user
> ls 			-- list all elements of the user
> replay "mongodb://172.17.42.1/mydb"	-- replay all changes of the user

Bind operations to duration:
> duration "2015-10-28T14:53:27"  "2015-10-29T14:53:27"
(all operations below are binded to this duration)
> sum		-- print number of records of the user in this duration
> ls 		-- show all elements of the user in this duration
> replay "mongodb://172.17.42.1/mydb"	-- replay all changes of the user in this duration
`

type ToolBox struct {
	dbs          []*bolt.DB // all opened boltdb
	fileid       int        // current selected db
	userid       int        // current selected userid
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
		db, err := bolt.Open(file, 0600, &bolt.Options{Timeout: 1 * time.Second, ReadOnly: true})
		if err != nil {
			log.Println(err)
			continue
		}
		t.dbs = append(t.dbs, db)
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
	case TK_P:
		t.cmd_p()
	case TK_HELP:
		t.cmd_help()
	case TK_CLEAR:
		t.cmd_clear()

	case TK_U:
		t.cmd_u()
	case TK_S:
		t.cmd_s()

	case TK_DURATION:
		t.cmd_duration()
	case TK_BIND:
		t.cmd_bind()
	case TK_COUNT:
		t.cmd_count()
	case TK_LS:
		t.cmd_ls()
	case TK_REPLAY:
		t.cmd_replay()
	default:
		fmt.Println("unkown command:", cmd)
	}
}
