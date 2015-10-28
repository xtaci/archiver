package main

import (
	redo "github.com/gonet2/nsq-redo"
	"testing"
	"time"
)

func TestRedo(t *testing.T) {
	r := redo.NewRedoRecord()
	r.SetUid(1)
	r.SetApi("test")
	r.SetTS(uint64(time.Now().Unix()))
	r.AddChange("test", "field_a", 1, 2)
	redo.Publish(r)
}
