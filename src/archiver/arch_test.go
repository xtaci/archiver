package main

import (
	redo "github.com/gonet2/libs/nsq-redo"
	"testing"
	"time"
)

type testdoc struct {
	Name string
	Age  int
}

func TestRedo(t *testing.T) {
	doc := testdoc{}
	for i := 0; i < 10; i++ {
		r := redo.NewRedoRecord(1, "test", uint64(time.Now().Unix()))
		doc.Name = "YYY"
		doc.Age = i
		r.AddChange("test", "field_a", doc)
		redo.Publish(r)
	}
}
