package main

import (
	"github.com/yuin/gopher-lua"
	"gopkg.in/readline.v1"
	"strings"
)

const (
	PS1 = "\033[1;31m> \033[0m"
	PS2 = "\033[1;31m>> \033[0m"
)

type REPL struct {
	L       *lua.LState // the lua virtual machine
	toolbox *ToolBox
	rl      *readline.Instance
}

func (repl *REPL) init() {
	repl.L = lua.NewState()
	repl.toolbox = &ToolBox{}
	repl.toolbox.init("/data")
	rl, err := readline.New(PS1)
	if err != nil {
		panic(err)
	}
	repl.rl = rl
}

func (repl *REPL) doREPL() {
	for {
		str, ok := repl.loadline()
		repl.rl.SetPrompt(PS1)
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
	line, err := repl.rl.Readline()
	if err != nil {
		return "", false
	}
	// try add return
	_, err = repl.L.LoadString("return " + line)
	if err == nil { // syntax ok
		return line, true
	} else { // syntax error
		return repl.multiline(line)
	}
}

func (repl *REPL) multiline(ml string) (string, bool) {
	for {
		// try it
		_, err := repl.L.LoadString(ml)
		if err == nil { // syntax ok
			return ml, true
		} else if !incomplete(err) { // syntax error
			return ml, true
		}

		repl.rl.SetPrompt(PS2)
		line, err := repl.rl.Readline()
		if err != nil {
			return "", false
		}
		ml = ml + "\n" + line
	}
}

func main() {
	repl := &REPL{}
	repl.init()
	repl.doREPL()
}
