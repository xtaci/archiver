package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	tb := &ToolBox{}
	tb.init("/data")
	fmt.Println(help)
	in := bufio.NewReader(os.Stdin)
	for {
		prompt(tb)
		cmd, err := in.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
		tb.parse_exec(cmd)
	}
}

func prompt(tb *ToolBox) {
	var ps string
	if tb.fileid != -1 {
		ps += fmt.Sprintf("file(%v)", tb.dbs[tb.fileid])
	}
	if tb.userid != -1 {
		ps += fmt.Sprintf("userid(%v)", tb.userid)
	}
	if tb.duration_set {
		ps += fmt.Sprintf("(%v - %v)", tb.duration_a, tb.duration_b)
	}
	ps += "> "
	fmt.Print(ps)
}
