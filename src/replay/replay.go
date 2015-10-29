package main

import (
	"bytes"
	"fmt"
	"github.com/boltdb/bolt"
	"io"
	"log"
	"os"
	"path/filepath"
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
	TK_COUNT
	TK_USERS
	TK_BIND
	TK_INFO
	TK_LPAREN
	TK_RPAREN
	TK_COMMA
	TK_DURATION
	TK_EOF
)

var cmds = map[string]int{
	"ls":    TK_LS,
	"help":  TK_HELP,
	"clear": TK_CLEAR,

	"users": TK_USERS,
	"info":  TK_INFO,

	"bind":     TK_BIND,
	"count":    TK_COUNT,
	"duration": TK_DURATION,
	"show":     TK_SHOW,
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

list all database files(sorted by time):
> ls
> help		-- print this text
> clear 	-- clear all bindings

Global operations to file:
> users(1)	-- print all users of file#1
> info(2)	-- print summary of file#2

Bind Operations to user:
> bind(1, 1234)		-- all operations below are binded to a file#1 & user#1234
> count			-- print number of records of the user
> show			-- show all elements of the user
> replay("mongodb://172.17.42.1/mydb")	-- replay all changes of the user

Bind operations to duration:
> duration("2015-10-28T14:53:27", "2015-10-29T14:53:27")	
(all operations below are binded to this duration)
> count		-- print number of records of the user in this duration
> show		-- show all elements of the user in this duration
> replay("mongodb://172.17.42.1/mydb")	-- replay all changes of the user in this duration
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

func (t *ToolBox) init(dir string) {
	t.cmd_clear()
	files, err := filepath.Glob(dir + "/*.RDO")
	if err != nil {
		log.Println(err)
		os.Exit(-1)
	}

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
	} else if r == '(' {
		return &token{typ: TK_LPAREN}
	} else if r == ')' {
		return &token{typ: TK_RPAREN}
	} else if r == ',' {
		return &token{typ: TK_COMMA}
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
	case TK_LS:
		t.cmd_ls()
	case TK_HELP:
		t.cmd_help()
	case TK_CLEAR:
		t.cmd_clear()

	case TK_USERS:
		t.cmd_users()
	case TK_INFO:
		t.cmd_info()

	case TK_DURATION:
		t.cmd_duration()
	case TK_BIND:
		t.cmd_bind()
	case TK_COUNT:
		t.cmd_count()
	case TK_SHOW:
		t.cmd_show()
	case TK_REPLAY:
		t.cmd_replay()
	default:
		fmt.Println("unkown command:", cmd)
	}
}
