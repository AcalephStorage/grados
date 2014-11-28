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

// AsyncIoCallback is the signature of a callback function that can be used on asynchronous operations. This can receive
// a number of arguments that is passed during async function calls or can contain result data from the asychronous
// operation.
type AsyncIoCallback func(args ...interface{})

// ASyncIoErrorCallback is the signature of a callback function that will be called when an asynchronous operation has
// an unexpected error.
type ASyncIoErrorCallback func(err error, args ...interface{})

// AsyncObject represents an object ready for asynchronous operations. Use AsyncMode from an object instance to create
// a valid AsyncObjet instance.
type AsyncObject struct {
	ioContext  C.rados_ioctx_t
	name       string
	completion C.rados_completion_t
	onComplete AsyncIoCallback
	onSafe     AsyncIoCallback
	onError    ASyncIoErrorCallback
	args       []interface{}
}

// AsyncMode Prepares the object for asynchronous operations. If callbacks are set to nil, the results will be ignored.
// args passed here will be passed to the callbacks.
func (o *Object) AsyncMode(onComplete, onSafe AsyncIoCallback, onError ASyncIoErrorCallback, args ...interface{}) *AsyncObject {
	a := &AsyncObject{
		ioContext:  o.ioContext,
		name:       o.name,
		onComplete: onComplete,
		onSafe:     onSafe,
		args:       args,
	}
	C.rados_aio_create_completion(nil, nil, nil, &a.completion)
	return a
}

// Release stops asynchronous mode. No asynchronous operations should be done to the AsyncObject after this is called.
func (ao *AsyncObject) Release() {
	C.rados_aio_release(ao.completion)
}

// Write performes a write operation asynchronously.
func (ao *AsyncObject) Write(data io.Reader, offset uint64) {
	go func() {
		oid := C.CString(ao.name)
		defer freeString(oid)
		bufAddr, bufLen := readerToBuf(data)
		ret := C.rados_aio_write(ao.ioContext, oid, ao.completion, bufAddr, C.size_t(bufLen), C.uint64_t(offset))
		hasErr := ao.processError(ret, fmt.Sprintf("Unable to write to object %s", ao.name))
		if !hasErr {
			ao.completeOperation()
		}
	}()
}

// WriteFull performs a full write operation asynchronously.
func (ao *AsyncObject) WriteFull(data io.Reader) {
	go func() {
		oid := C.CString(ao.name)
		defer freeString(oid)
		bufAddr, bufLen := readerToBuf(data)
		ret := C.rados_aio_write_full(ao.ioContext, oid, ao.completion, bufAddr, C.size_t(bufLen))
		if err := toRadosError(ret); err != nil {
			err.Message = fmt.Sprintf("Unable to write full to object %s", ao.name)
			ao.onError(err, ao.args...)
			return
		}
		ao.completeOperation()
	}()
}

// Append performs an append operation asynchronously.
func (ao *AsyncObject) Append(data io.Reader) {
	go func() {
		oid := C.CString(ao.name)
		defer freeString(oid)
		bufAddr, bufLen := readerToBuf(data)
		ret := C.rados_aio_append(ao.ioContext, oid, ao.completion, bufAddr, C.size_t(bufLen))
		hasErr := ao.processError(ret, fmt.Sprintf("Unable to append to object %s", ao.name))
		if !hasErr {
			ao.completeOperation()
		}
	}()
}

// Read reads from the object a specific length starting at the given offset. The read data is stored in an io.Reader
// and is appended at the end of the args passed to the onComplete and onSafe callbacks.
func (ao *AsyncObject) Read(length, offset uint64) {
	go func() {
		oid := C.CString(ao.name)
		defer freeString(oid)
		bufAddr := bufferAddress(int(length))
		ret := C.rados_aio_read(ao.ioContext, oid, ao.completion, bufAddr, C.size_t(length), C.uint64_t(offset))
		hasErr := ao.processError(ret, fmt.Sprintf("Unable to read from object %s", ao.name))
		if hasErr {
			return
		}
		go func() {
			C.rados_aio_wait_for_complete(ao.completion)
			ret = C.rados_aio_get_return_value(ao.completion)
			data := bufToReader(bufAddr, ret)
			ao.args = append(ao.args, data)
			argCopy := make([]interface{}, len(ao.args))
			copy(argCopy, ao.args)
			ao.onComplete(argCopy...)
		}()
		go func() {
			C.rados_aio_wait_for_safe(ao.completion)
			ret = C.rados_aio_get_return_value(ao.completion)
			data := bufToReader(bufAddr, ret)
			ao.args = append(ao.args, data)
			argCopy := make([]interface{}, len(ao.args))
			copy(argCopy, ao.args)
			ao.onSafe(argCopy...)
		}()
	}()

}

// Remove removes an object asynchronously.
func (ao *AsyncObject) Remove() {
	go func() {
		oid := C.CString(ao.name)
		defer freeString(oid)
		ret := C.rados_aio_remove(ao.ioContext, oid, ao.completion)
		hasErr := ao.processError(ret, fmt.Sprintf("Unable to remove object %s", ao.name))
		if !hasErr {
			ao.completeOperation()
		}
	}()
}

// helper method to call the error callback.
func (ao *AsyncObject) processError(ret C.int, msg string) bool {
	if err := toRadosError(ret); err != nil {
		err.Message = msg
		if ao.onError != nil {
			ao.onError(err, ao.args...)
		}
		return true
	}
	return false
}

// helper method to call the onComplete and onsafe callbacks.
func (ao *AsyncObject) completeOperation() {
	go func() {
		if ao.onComplete != nil {
			C.rados_aio_wait_for_complete(ao.completion)
			argCopy := make([]interface{}, len(ao.args))
			copy(argCopy, ao.args)
			ao.onComplete(argCopy...)
		}
	}()
	go func() {
		if ao.onSafe != nil {
			C.rados_aio_wait_for_safe(ao.completion)
			argCopy := make([]interface{}, len(ao.args))
			copy(argCopy, ao.args)
			ao.onSafe(argCopy...)
		}
	}()

}
