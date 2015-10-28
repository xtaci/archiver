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
	if tb.dbid != -1 {
		fmt.Printf("%v> ", tb.dbs[tb.dbid])
	} else {
		fmt.Print("> ")
	}
}
