package main

import (
	redo "github.com/gonet2/libs/nsq-redo"
	"testing"
	"time"
)

func TestRedo(t *testing.T) {
	for i := 0; i < 10; i++ {
		r := redo.NewRedoRecord(1, "test", uint64(time.Now().Unix()))
		r.AddChange("test", "field_a", 2)
		redo.Publish(r)
	}
}
