package grados

/*
#cgo LDFLAGS: -lrados

#include <stdlib.h>
#include <rados/librados.h>
*/
import "C"

import (
	"bytes"
	"fmt"
	"io"
	"time"
	"unsafe"
)

// SetLocatorKey sets the key for mapping objects to pgs.
//
// The key is used instead of the object name to determine which placement groups an object is put in. This affects all
// subsequent operations of the io context - until a different locator key is set, all objects in this io context will
// be placed in the same pg.
//
// This is useful if you need to do clone_range operations, which must be done with the source and destination objects
// in the same pg.
func (pool *Pool) SetLocatorKey(key string) {
	k := C.CString(key)
	defer C.free(unsafe.Pointer(k))
	C.rados_ioctx_locator_set_key(pool.context, k)
}

// Set the namespace for objects.
//
// The namespace specification further refines a pool into different domains. The mapping of objects to pgs is also
// based on this value.
func (pool *Pool) SetNamespace(namespace string) {
	n := C.CString(namespace)
	defer C.free(unsafe.Pointer(n))
	C.rados_ioctx_set_namespace(pool.context, n)
}

// LastObjectVersion returns the version of the last object read or written.
func (pool *Pool) LastObjectVersion() uint64 {
	return uint64(C.rados_get_last_version(pool.context))
}

// WriteToObject writes the data at a specific offset to the given objectId.
func (pool *Pool) WriteToObject(objectId string, data io.Reader, offset uint64) error {
	dataBuf := new(bytes.Buffer)
	dataBuf.ReadFrom(data)

	oid := C.CString(objectId)
	defer C.free(unsafe.Pointer(oid))

	bufAddr := (*C.char)(unsafe.Pointer(&dataBuf.Bytes()[0]))

	ret := C.rados_write(pool.context, oid, bufAddr, C.size_t(dataBuf.Len()), C.uint64_t(offset))
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to write data to object %s", objectId)
		return err
	}
	return nil
}

// WriteFullToObject writes the data to object replacing old data
func (pool *Pool) WriteFullToObject(objectId string, data io.Reader) error {
	dataBuf := new(bytes.Buffer)
	dataBuf.ReadFrom(data)

	oid := C.CString(objectId)
	defer C.free(unsafe.Pointer(oid))

	bufAddr := (*C.char)(unsafe.Pointer(&dataBuf.Bytes()[0]))
	ret := C.rados_write_full(pool.context, oid, bufAddr, C.size_t(dataBuf.Len()))
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to write full data to object %s", objectId)
		return err
	}
	return nil
}

// AppendToObject appends new data to an object
func (pool *Pool) AppendToObject(objectId string, data io.Reader) error {
	dataBuf := new(bytes.Buffer)
	dataBuf.ReadFrom(data)

	oid := C.CString(objectId)
	defer C.free(unsafe.Pointer(oid))
	bufAddr := (*C.char)(unsafe.Pointer(&dataBuf.Bytes()[0]))
	ret := C.rados_append(pool.context, oid, bufAddr, C.size_t(dataBuf.Len()))
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to append data to object %s", objectId)
		return err
	}
	return nil
}

// ReadFromObject reads a specified length of data from the object starting at the given offset.
func (pool *Pool) ReadFromObject(objectId string, length, offset uint64) (io.Reader, error) {
	oid := C.CString(objectId)
	defer C.free(unsafe.Pointer(oid))

	buf := make([]byte, int(length))
	bufAddr := unsafe.Pointer(&buf[0])

	ret := C.rados_read(pool.context, oid, (*C.char)(bufAddr), C.size_t(length), C.uint64_t(offset))
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to read object %s.", objectId)
		return nil, err
	}
	data := C.GoBytes(bufAddr, ret)
	dataBuf := bytes.NewBuffer(data)
	return dataBuf, nil
}

// RemoveObject removes an object from the pool.
func (pool *Pool) RemoveObject(objectId string) error {
	oid := C.CString(objectId)
	defer C.free(unsafe.Pointer(oid))
	ret := C.rados_remove(pool.context, oid)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to delete object %s", objectId)
		return err
	}
	return nil
}

// ResizeObject modifies the size of an object. If the object size is increases, the new space is zero-filled. If the
// size is reduced, the excess data is removed.
func (pool *Pool) ResizeObject(objectId string, newSize uint64) error {
	oid := C.CString(objectId)
	defer C.free(unsafe.Pointer(oid))
	size := C.uint64_t(newSize)
	ret := C.rados_trunc(pool.context, oid, size)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to resize object %s to size %d.", objectId, newSize)
		return err
	}
	return nil
}

// CloneObject clones a length of data from an object given an offest to another object starting at an offset. The source
// and destination objects must be on the same PG. This requires that a locator key must be set first.
func (pool *Pool) CloneObject(srcObjId string, srcOffset uint64, destObjId string, destOffset, length uint64) error {
	srcOid := C.CString(srcObjId)
	srcOff := C.uint64_t(srcOffset)
	defer C.free(unsafe.Pointer(srcOid))

	destOid := C.CString(destObjId)
	destOff := C.uint64_t(destOffset)
	defer C.free(unsafe.Pointer(destOid))

	ret := C.rados_clone_range(pool.context, destOid, destOff, srcOid, srcOff, C.size_t(length))
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to clone %s to %s.", srcObjId, destObjId)
		return err
	}
	return nil
}

type ObjectStatus struct {
	size         uint64
	modifiedTime time.Time
}

func (pool *Pool) ObjectStatus(objectId string) (*ObjectStatus, error) {
	oid := C.CString(objectId)
	defer C.free(unsafe.Pointer(oid))
	var objectSize C.uint64_t
	var modifiedTime C.time_t
	ret := C.rados_stat(pool.context, oid, &objectSize, &modifiedTime)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to get status for object %s.", objectId)
		return nil, err
	}
	return &ObjectStatus{
		size:         uint64(objectSize),
		modifiedTime: time.Unix(int64(modifiedTime), 0),
	}, nil
}
