package grados

/*
#cgo LDFLAGS: -lrados
#include <rados/librados.h>
*/
import "C"

import (
	"fmt"
	"io"
)

type ReadOperation struct {
	ioContext C.rados_ioctx_t
	opContext C.rados_read_op_t
	buffer    *C.char
	bytesRead C.size_t
	retVal    C.int
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
	bufAddr, bufLen := readerToBuf(value)
	C.rados_read_op_cmpxattr(ro.opContext, n, C.uint8_t(operator), bufAddr, C.size_t(bufLen))
	return ro
}

func (ro *ReadOperation) Read(offset, length uint64) {
	ro.buffer = bufferAddress(int(length))
	C.rados_read_op_read(ro.opContext, C.uint64_t(offset), C.size_t(length), ro.buffer, &ro.bytesRead, &ro.retVal)
}

func (ro *ReadOperation) Operate(object *Object, flags ...LibradosOperation) (io.Reader, error) {
	oid := C.CString(object.name)
	defer freeString(oid)
	var f C.int = 0
	for _, flag := range flags {
		f |= C.int(flag)
	}
	ret := C.rados_read_op_operate(ro.opContext, ro.ioContext, oid, f)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to perform read operations on object %s.", object.name)
		return nil, err
	}
	if err := toRadosError(ro.retVal); err != nil {
		err.Message = fmt.Sprintf("Unable to read from object %s.", object.name)
		return nil, err
	}
	if ro.bytesRead == 0 {
		err := toRadosError(-1)
		err.Message = fmt.Sprintf("Nothing read from object %s.", object.name)
		return nil, err
	}
	data := bufToReader(ro.buffer, C.int(ro.bytesRead))
	return data, nil
}
