package main

import (
	"testing"
)

func TestParser(t *testing.T) {
	tb := ToolBox{}
	tb.init("/data")

	tb.parse_exec("ls")
	tb.parse_exec("mgo abc")
	tb.parse_exec("show(1,2,3)")
	tb.parse_exec("show(1,2)")
	tb.parse_exec("replay(1,2)")
	tb.parse_exec("replay(1,2,3)")
	t.Log(t)
}
