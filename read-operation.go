package grados

/*
#cgo LDFLAGS: -lrados
#include <rados/librados.h>
*/
import "C"

import (
	"io"
)

type ReadOperation struct {
	ioContext C.rados_ioctx_t
	opContext C.rados_read_op_t
}

func (pool *Pool) CreateReadOperation() (*ReadOperation, error) {
	opContext := C.rados_create_read_op()
	if opContext == nil {
		err := toRadosError(-1)
		err.Message = "Unable to create read operation."
		return nil, err
	}
	ro := &ReadOperation{
		ioContext: pool.context,
		opContext: opContext,
	}
	return ro, nil
}

func (ro *ReadOperation) Release() {
	C.rados_release_read_op(ro.opContext)
}

func (ro *ReadOperation) SetFlags(flags ...LibradosOpFlag) *ReadOperation {
	var f C.int = 0
	for _, flag := range flags {
		f |= C.int(flag)
	}
	C.rados_read_op_set_flags(ro.opContext, f)
	return ro
}

func (ro *ReadOperation) AssertExists() *ReadOperation {
	C.rados_read_op_assert_exists(ro.opContext)
	return ro
}

func (ro *ReadOperation) CompareAttribute(name string, operator CompareAttribute, value io.Reader) *ReadOperation {
	n := C.CString(name)
	defer freeString(n)
	return ro
}
