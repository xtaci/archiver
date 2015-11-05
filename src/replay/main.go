package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
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
	ps += "> "
	fmt.Print(ps)
}
