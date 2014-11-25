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
	"unsafe"
)

type AttributeList struct {
	iterator C.rados_xattrs_iter_t
}

func (pool *Pool) OpenAttributeList(objectId string) (*AttributeList, error) {
	oid := C.CString(objectId)
	defer C.free(unsafe.Pointer(oid))
	var iterator C.rados_xattrs_iter_t
	ret := C.rados_getxattrs(pool.context, oid, &iterator)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to retrieve attributes of object %s", objectId)
		return nil, err
	}
	return &AttributeList{
		iterator: iterator,
	}, nil
}

func (i *AttributeList) Next() (name string, value io.Reader, err error) {
	var n *C.char
	var v *C.char
	var length C.size_t
	ret := C.rados_getxattrs_next(i.iterator, &n, &v, &length)
	if errs := toRadosError(ret); errs != nil {
		errs.Message = "Unable to get next attribute"
		err = errs
	} else if length == 0 {
		errs := toRadosError(-1)
		errs.Message = "End of attribute list reached"
		err = errs
	} else {
		name = C.GoString(n)
		val := C.GoBytes(unsafe.Pointer(v), C.int(length))
		value = bytes.NewBuffer(val)
	}
	return
}

func (i *AttributeList) End() {
	C.rados_getxattrs_end(i.iterator)
}

func (pool *Pool) ObjectAttribute(objectName, attributeName string) (io.Reader, error) {
	object := C.CString(objectName)
	attribute := C.CString(attributeName)
	defer C.free(unsafe.Pointer(object))
	defer C.free(unsafe.Pointer(attribute))

	buf := make([]byte, 2048)
	bufAddr := unsafe.Pointer(&buf[0])

	ret := C.rados_getxattr(pool.context, object, attribute, (*C.char)(bufAddr), C.size_t(len(buf)))
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to get attribute %s of object %s.", attributeName, objectName)
		return nil, err
	}
	attrib := C.GoBytes(bufAddr, ret)
	attribBuf := bytes.NewBuffer(attrib)
	return attribBuf, nil
}

func (pool *Pool) SetObjectAttribute(objectName, attributeName string, attributeValue io.Reader) error {
	object := C.CString(objectName)
	attribute := C.CString(attributeName)
	defer C.free(unsafe.Pointer(object))
	defer C.free(unsafe.Pointer(attribute))

	atrribValBuf := new(bytes.Buffer)
	atrribValBuf.ReadFrom(attributeValue)

	bufAddr := (*C.char)(unsafe.Pointer(&atrribValBuf.Bytes()[0]))

	ret := C.rados_setxattr(pool.context, object, attribute, bufAddr, C.size_t(atrribValBuf.Len()))
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to set attribute %s=%s to object %s.", attributeName, atrribValBuf.String(), objectName)
		return err
	}
	return nil
}

func (pool *Pool) RemoveObjectAttribute(objectName, attributeName string) error {
	object := C.CString(objectName)
	attribute := C.CString(attributeName)
	defer C.free(unsafe.Pointer(object))
	defer C.free(unsafe.Pointer(attribute))

	ret := C.rados_rmxattr(pool.context, object, attribute)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to get attribute %s of object %s.", attributeName, objectName)
		return err
	}
	return nil
}
