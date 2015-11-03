package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	fmt.Println(help)
	tb := &ToolBox{}
	tb.init("/data")
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
	if tb.userid != -1 {
		ps += fmt.Sprintf("\033[0;32mid(%v)\033[0m", tb.userid)
	}
	if tb.duration_set {
		ps += fmt.Sprintf("\033[1m(%v -- %v)\033[0m", tb.duration_a, tb.duration_b)
	}
	ps += "> "
	fmt.Print(ps)
}
