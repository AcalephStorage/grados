package grados

/*
#cgo LDFLAGS: -lrados

#include <stdlib.h>
#include <rados/librados.h>
*/
import "C"

import (
	"bytes"
	"io"
	"time"
	"unsafe"
)

type WriteOperation struct {
	ioContext C.rados_ioctx_t
	opContext C.rados_write_op_t
}

func (pool *Pool) CreateWriteOperation() (*WriteOperation, error) {
	opContext := C.rados_create_write_op()
	if opContext == nil {
		err := toRadosError(-1)
		err.Message = "Unable to create write operation."
		return nil, err
	}
	wo := &WriteOperation{
		ioContext: pool.context,
		opContext: opContext,
	}
	return wo, nil
}

func (wo *WriteOperation) Release() {
	C.rados_release_write_op(wo.opContext)
}

func (wo *WriteOperation) SetFlags(flags ...LibradosOpFlag) *WriteOperation {
	var f C.int = 0
	for _, flag := range flags {
		f |= C.int(flag)
	}
	C.rados_write_op_set_flags(wo.opContext, f)
	return wo
}

func (wo *WriteOperation) AssertExists() *WriteOperation {
	C.rados_write_op_assert_exists(wo.opContext)
	return wo
}

func (wo *WriteOperation) CompareAttribute(attributeName string, operator CompareAttribute, value io.Reader) *WriteOperation {
	name := C.CString(attributeName)
	defer C.free(unsafe.Pointer(name))

	buf := new(bytes.Buffer)
	buf.ReadFrom(value)

	bufAddr := (*C.char)(unsafe.Pointer(&buf.Bytes()[0]))

	C.rados_write_op_cmpxattr(wo.opContext, name, C.uint8_t(operator), bufAddr, C.size_t(buf.Len()))
	return wo
}

func (wo *WriteOperation) SetAttribute(name string, value io.Reader) *WriteOperation {
	n := C.CString(name)
	defer C.free(unsafe.Pointer(n))

	buf := new(bytes.Buffer)
	buf.ReadFrom(value)
	bufAddr := (*C.char)(unsafe.Pointer(&buf.Bytes()[0]))

	C.rados_write_op_setxattr(wo.opContext, n, bufAddr, C.size_t(buf.Len()))
	return wo
}

func (wo *WriteOperation) RemoveAttribute(name string) *WriteOperation {
	n := C.CString(name)
	defer C.free(unsafe.Pointer(n))
	C.rados_write_op_rmxattr(wo.opContext, n)
	return wo
}

func (wo *WriteOperation) CreateObject(mode CreateMode, category string) *WriteOperation {
	c := C.CString(category)
	defer C.free(unsafe.Pointer(c))

	C.rados_write_op_create(wo.opContext, C.int(mode), c)
	return wo
}

func (wo *WriteOperation) Write(data io.Reader, offset uint64) *WriteOperation {
	buf := new(bytes.Buffer)
	buf.ReadFrom(data)
	bufAddr := (*C.char)(unsafe.Pointer(&buf.Bytes()[0]))
	C.rados_write_op_write(wo.opContext, bufAddr, C.size_t(buf.Len()), C.uint64_t(offset))
	return wo
}

func (wo *WriteOperation) WriteFull(data io.Reader) *WriteOperation {
	buf := new(bytes.Buffer)
	buf.ReadFrom(data)
	bufAddr := (*C.char)(unsafe.Pointer(&buf.Bytes()[0]))
	C.rados_write_op_write_full(wo.opContext, bufAddr, C.size_t(buf.Len()))
	return wo
}

func (wo *WriteOperation) Append(data io.Reader) *WriteOperation {
	buf := new(bytes.Buffer)
	buf.ReadFrom(data)
	bufAddr := (*C.char)(unsafe.Pointer(&buf.Bytes()[0]))
	C.rados_write_op_append(wo.opContext, bufAddr, C.size_t(buf.Len()))
	return wo
}

func (wo *WriteOperation) Remove() *WriteOperation {
	C.rados_write_op_remove(wo.opContext)
	return wo
}

func (wo *WriteOperation) Truncate(offset uint64) *WriteOperation {
	C.rados_write_op_truncate(wo.opContext, C.uint64_t(offset))
	return wo
}

func (wo *WriteOperation) Zero(offset, length uint64) *WriteOperation {
	C.rados_write_op_zero(wo.opContext, C.uint64_t(offset), C.uint64_t(length))
	return wo
}

func (wo *WriteOperation) Operate(objectId string, modifiedTime *time.Time, flags ...LibradosOperation) error {
	oid := C.CString(objectId)
	defer C.free(unsafe.Pointer(oid))
	var mtime C.time_t
	if modifiedTime != nil {
		mtime = C.time_t(modifiedTime.Unix())
	}
	var f C.int = 0
	for _, flag := range flags {
		f |= C.int(flag)
	}
	ret := C.rados_write_op_operate(wo.opContext, wo.ioContext, oid, &mtime, f)
	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to perform write operation."
		return err
	}
	return nil
}
