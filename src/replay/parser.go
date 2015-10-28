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
)

var cmds = map[string]int{
	"ls":     TK_LS,
	"select": TK_SELECT,
}

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
