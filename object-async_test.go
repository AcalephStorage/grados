package grados

import "testing"
import "bytes"

func TestAsyncWriteAppend(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}

	pool, err := cluster.ManagePool("data")
	handleError(t, err)
	if pool == nil {
		return
	}

	c := make(chan int)
	object := pool.ManageObject("sample").AsyncMode(onComplete, onSafe, onError, t, c, "hello", "world", 1, 2)
	object.Write(bytes.NewBufferString("Writing this sample"), 0)
	object.Append(bytes.NewBufferString("just appending"))

	t.Log("waiting for completion")
	<-c
	t.Log("completed once")
	<-c
	t.Log("completed twice")
	<-c
	t.Log("safe once")
	<-c
	t.Log("safe twice")

	cluster.Shutdown()
}

func onComplete(args ...interface{}) {
	t := args[0].(*testing.T)
	z := args[1].(chan int)
	t.Log("async complete")
	z <- 1
}

func onSafe(args ...interface{}) {
	t := args[0].(*testing.T)
	z := args[1].(chan int)
	t.Log("async safe")
	z <- 1
}

func onError(err error, args ...interface{}) {
	t := args[0].(*testing.T)
	z := args[1].(chan int)
	t.Errorf("write error: %s", err)
	z <- 0
	z <- 0
}
