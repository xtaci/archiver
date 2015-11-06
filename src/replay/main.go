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
	fmt.Println(err)
	if err == nil { // syntax ok
		return line, true
	} else { // syntax error
		return repl.multiline(line)
	}
}

func (repl *REPL) multiline(ml string) (string, bool) {
	for {
		fmt.Print(">> ")
		line, err := repl.in.ReadString('\n')
		if err != nil {
			return "", false
		}
		ml = ml + "\n" + line

		_, err = repl.L.LoadString(ml)
		fmt.Println("error:", err)
		if err == nil { // syntax ok
			return ml, true
		} else if !incomplete(err) { // syntax error
			return ml, true
		}
	}
}

func main() {
	repl := &REPL{}
	repl.init()
	repl.doREPL()
}
