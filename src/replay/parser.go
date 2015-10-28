package main

import (
	"github.com/boltdb/bolt"
	"log"
	"os"
	"path"
	"path/filepath"
)

const (
	TK_LS = iota
	TK_SELECT
	TK_REPLAY
	TK_SHOW
	TK_MGO
)

var cmds = map[string]int{
	"ls":     TK_LS,
	"select": TK_SELECT,
	"replay": TK_REPLAY,
	"show":   TK_SHOW,
	"mgo":    TK_MGO,
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
	dbs      map[string]*bolt.DB // all opened boltdb
	selected string              // current selected db
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

func (t *ToolBox) exec(string) {
}

//////////////////////////////////////////
// parser
func (t *ToolBox) parsse(string) {
}
