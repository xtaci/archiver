package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"time"
)

const (
	BOLTDB_BUCKET = "REDOLOG"
	LAYOUT        = "2006-01-02T15:04:05"
)

func (t *ToolBox) cmd_ls() {
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

func (t *ToolBox) cmd_users() {
	t.match(TK_LPAREN)
	fileid_tk := t.match(TK_NUM)
	t.match(TK_RPAREN)

	fmt.Println("TODO: list users of this db:", fileid_tk)
}

func (t *ToolBox) cmd_info() {
	t.match(TK_LPAREN)
	fileid_tk := t.match(TK_NUM)
	t.match(TK_RPAREN)

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
	t.match(TK_LPAREN)
	fileid_tk := t.match(TK_NUM)
	t.match(TK_COMMA)
	userid_tk := t.match(TK_NUM)
	t.match(TK_RPAREN)

	if fileid_tk.num < len(t.dbs) {
		t.fileid = fileid_tk.num
		t.userid = userid_tk.num
		return
	}
	fmt.Println("no such file", fileid_tk.num)
}

func (t *ToolBox) cmd_duration() {
	t.match(TK_LPAREN)
	tk_a := t.match(TK_STRING)
	t.match(TK_COMMA)
	tk_b := t.match(TK_STRING)
	t.match(TK_RPAREN)

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
	fmt.Println("TODO: implement count")
}

func (t *ToolBox) cmd_show() {
	fmt.Println("TODO: implement show")
}

func (t *ToolBox) cmd_replay() {
	fmt.Println("TODO: implement replay")
}
