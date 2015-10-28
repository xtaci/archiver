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
	"unicode"
)

const (
	TK_UNDEFINED = iota
	TK_LS
	TK_SELECT
	TK_REPLAY
	TK_SHOW
	TK_NUM
	TK_MGO
	TK_LPAREN
	TK_RPAREN
	TK_COMMA
	TK_EOF
)

var cmds = map[string]int{
	"ls":     TK_LS,
	"select": TK_SELECT,
	"replay": TK_REPLAY,
	"show":   TK_SHOW,
	"mgo":    TK_MGO,
}

type token struct {
	typ     int
	literal string
	num     int
}

// set ts=8
var help = `REDO Replay Tool

Commands:
	ls 				-- list all database files
	mgo mongodb://xxx/mydb		-- define mongodb url for replay

	select 1				-- choose a file
		show(1, 100)			-- show all elements from 1, count 100
		show(1234, 1,100)		-- show elements for a user(1234) from 1, count 100
		replay(1234, 50, 100)		-- replay to user(1234) from 50 count 50
		replay(1234)			-- replay all changes to a user(1234) 
`

type ToolBox struct {
	dbs        []*bolt.DB    // all opened boltdb
	selected   int           // current selected db
	cmd_reader *bytes.Buffer // cmds
	mgo_url    string
}

func (t *ToolBox) init(dir string) {
	files, err := filepath.Glob(dir + "/*.RDO")
	if err != nil {
		log.Println(err)
		os.Exit(-1)
	}

	for _, file := range files {
		db, err := bolt.Open(file, 0600, nil)
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
	} else {
		return &token{}
	}
	return nil
}

func (t *ToolBox) read2end() string {
	var runes []rune
	for {
		r, _, err := t.cmd_reader.ReadRune()
		if err == io.EOF {
			break
		} else if r == '\r' || r == '\n' {
			break
		} else {
			runes = append(runes, r)
		}
	}

	return string(runes)
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
	case TK_SELECT:
		t.cmd_select()
	case TK_REPLAY:
		t.cmd_replay()
	case TK_MGO:
		t.cmd_mgo()
	case TK_SHOW:
		t.cmd_show()
	default:
		fmt.Println("syntax err:", cmd)
	}
}

func (t *ToolBox) cmd_ls() {
	for k, v := range t.dbs {
		fmt.Printf("%v -- %v\n", k, v)
	}
}

func (t *ToolBox) cmd_select() {
	tk := t.match(TK_NUM)
	if tk.num < len(t.dbs) {
		t.selected = tk.num
	} else {
		fmt.Println("no such index")
	}
}

func (t *ToolBox) cmd_mgo() {
	t.mgo_url = t.read2end()
}

func (t *ToolBox) cmd_show() {
	var param []int

	t.match(TK_LPAREN)
	tk := t.match(TK_NUM)
	param = append(param, tk.num)
	t.match(TK_COMMA)
	tk = t.match(TK_NUM)
	param = append(param, tk.num)
	tk = t.next()
	if tk.typ == TK_COMMA {
		tk = t.match(TK_NUM)
		param = append(param, tk.num)
		t.match(TK_RPAREN)
	} else if tk.typ == TK_RPAREN {
	} else {
		panic("syntax error")
	}

	fmt.Println("params:", param)
}

func (t *ToolBox) cmd_replay() {
	var param []int

	t.match(TK_LPAREN)
	tk := t.match(TK_NUM)
	param = append(param, tk.num)
	t.match(TK_COMMA)
	tk = t.match(TK_NUM)
	param = append(param, tk.num)
	tk = t.next()
	if tk.typ == TK_COMMA {
		tk = t.match(TK_NUM)
		param = append(param, tk.num)
		t.match(TK_RPAREN)
	} else if tk.typ == TK_RPAREN {
	} else {
		panic("syntax error")
	}

	fmt.Println("params:", param)
}
