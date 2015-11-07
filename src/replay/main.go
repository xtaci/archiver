package main

import (
	"github.com/yuin/gopher-lua"
	"gopkg.in/readline.v1"
	"log"
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

func NewREPL() *REPL {
	repl := new(REPL)
	repl.L = lua.NewState()
	repl.toolbox = NewToolBox("/data")
	if rl, err := readline.New(PS1); err == nil {
		repl.rl = rl
	} else {
		log.Println(err)
		return nil
	}
	return repl
}

// read/eval/print/loop
func (repl *REPL) start() {
	defer func() {
		repl.rl.Close()
	}()

	for {
		if str, err := repl.loadline(); err == nil {
			repl.toolbox.exec(str)
		} else {
			log.Println(err)
			break
		}
	}
}

func incomplete(err error) bool {
	if strings.Index(err.Error(), "EOF") != -1 {
		return true
	}
	return false
}

func (repl *REPL) loadline() (string, error) {
	repl.rl.SetPrompt(PS1)
	if line, err := repl.rl.Readline(); err == nil {
		if _, err := repl.L.LoadString("return " + line); err == nil { // try add return <...> then compile
			return line, nil
		} else {
			return repl.multiline(line)
		}
	} else {
		return "", err
	}
}

func (repl *REPL) multiline(ml string) (string, error) {
	for {
		if _, err := repl.L.LoadString(ml); err == nil { // try compile
			return ml, nil
		} else if !incomplete(err) { // syntax error, but not EOF
			return ml, nil
		} else { // read next line
			repl.rl.SetPrompt(PS2)
			if line, err := repl.rl.Readline(); err == nil {
				ml = ml + "\n" + line
			} else {
				return "", err
			}
		}
	}
}

func main() {
	repl := NewREPL()
	repl.start()
}
