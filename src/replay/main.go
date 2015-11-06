package main

import (
	"bufio"
	"fmt"
	"github.com/yuin/gopher-lua"
	"os"
	"strings"
)

type REPL struct {
	L       *lua.LState // the lua virtual machine
	in      *bufio.Reader
	toolbox *ToolBox
	linebuf string
}

func (repl *REPL) init() {
	repl.L = lua.NewState()
	repl.toolbox = &ToolBox{}
	repl.toolbox.init("/data")
	repl.in = bufio.NewReader(os.Stdin)
}

func (repl *REPL) doREPL() {
	for {
		str, ok := repl.loadline()
		if !ok {
			break
		}
		repl.toolbox.exec(str)
	}
}

func incomplete(err error) bool {
	if strings.Index(err.Error(), "EOF") != -1 {
		return true
	}
	return false
}

func (repl *REPL) loadline() (string, bool) {
	fmt.Print("> ")
	line, err := repl.in.ReadString('\n')
	if err != nil {
		return "", false
	}
	// try add return
	_, err = repl.L.LoadString("return " + line)
	if err == nil { // syntax ok
		return line, true
	} else {
		if incomplete(err) { // non-terminated, try multiline
			repl.linebuf = repl.linebuf + "\n" + line
			return repl.multiline()
		} else { // syntax error
			return line, true
		}
	}
}

func (repl *REPL) multiline() (string, bool) {
	for {
		fmt.Print(">> ")
		line, err := repl.in.ReadString('\n')
		if err != nil {
			return "", false
		}
		repl.linebuf = repl.linebuf + "\n" + line

		_, err = repl.L.LoadString(repl.linebuf)
		if err == nil { // syntax ok
			return repl.linebuf, true
		} else if !incomplete(err) { // syntax error
			return repl.linebuf, true
		}
	}
}

func main() {
	repl := &REPL{}
	repl.init()
	repl.doREPL()
}
