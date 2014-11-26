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

type AttributeList struct {
	iterator C.rados_xattrs_iter_t
}

func (o *Object) OpenAttributeList() (*AttributeList, error) {
	oid := C.CString(o.Name)
	defer freeString(oid)
	var iterator C.rados_xattrs_iter_t
	ret := C.rados_getxattrs(o.ioContext, oid, &iterator)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to retrieve attributes of object %s", o.Name)
		return nil, err
	}
	al := &AttributeList{
		iterator: iterator,
	}
	return al, nil
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
		value = bufToReader(v, C.int(length))
	}
	return
}

func (i *AttributeList) Close() {
	C.rados_getxattrs_end(i.iterator)
}

func (o *Object) Attribute(attributeName string) (io.Reader, error) {
	object := C.CString(o.Name)
	attribute := C.CString(attributeName)
	defer freeString(object)
	defer freeString(attribute)

	bufLen := 2048
	bufAddr := bufferAddress(bufLen)

	ret := C.rados_getxattr(o.ioContext, object, attribute, bufAddr, C.size_t(bufLen))
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to get attribute %s of object %s.", attributeName, o.Name)
		return nil, err
	}
	attribbuf := bufToReader(bufAddr, ret)
	return attribbuf, nil
}

func (o *Object) SetAttribute(attributeName string, attributeValue io.Reader) error {
	object := C.CString(o.Name)
	attribute := C.CString(attributeName)
	defer freeString(object)
	defer freeString(attribute)

	bufAddr, bufLen := readerToBuf(attributeValue)

	ret := C.rados_setxattr(o.ioContext, object, attribute, bufAddr, bufLen)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to set attribute %s of object %s.", attributeName, o.Name)
		return err
	}
	return nil
}

func (o *Object) RemoveAttribute(attributeName string) error {
	object := C.CString(o.Name)
	attribute := C.CString(attributeName)
	defer freeString(object)
	defer freeString(attribute)

	ret := C.rados_rmxattr(o.ioContext, object, attribute)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to get attribute %s of object %s.", attributeName, o.Name)
		return err
	}
	return nil
}
