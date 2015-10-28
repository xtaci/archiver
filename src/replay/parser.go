package main

import (
	"bytes"
	"github.com/boltdb/bolt"
	"io"
	"log"
	"os"
	"path"
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

var help = `REDO Replay Tool

Commands:
	ls 				-- list all database files
	mgo mongodb://xxx/mydb		-- define mongodb url for replay

	select xxx.RDO				-- choose a file
		show(1, 100)			-- show all elements from 1, count 100
		show(1234, 1,100)		-- show elements for a user(1234) from 1, count 100
		replay(1234, 50, 100)		-- replay to user(1234) from 50 count 50
		replay(1234)			-- replay all changes to a user(1234) 
`

type ToolBox struct {
	dbs        map[string]*bolt.DB // all opened boltdb
	selected   string              // current selected db
	cmd_reader *bytes.Buffer       // cmds
}

func (t *ToolBox) init(dir string) {
	t.dbs = make(map[string]*bolt.DB)
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
		t.dbs[path.Base(file)] = db
	}
}

func (t *ToolBox) exec(cmd string) {
	t.cmd_reader = bytes.NewBufferString(cmd)
	t.parse(cmd)
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

func (t *ToolBox) parse(cmd string) {

}
