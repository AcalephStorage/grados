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

// AttributeList is an iterator to an object's extended attributes.
type AttributeList struct {
	iterator C.rados_xattrs_iter_t
}

// OpenAttributeList returns an iterator of the object's attributes.
func (o *Object) OpenAttributeList() (*AttributeList, error) {
	oid := C.CString(o.name)
	defer freeString(oid)
	var iterator C.rados_xattrs_iter_t
	ret := C.rados_getxattrs(o.ioContext, oid, &iterator)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to retrieve attributes of object %s", o.name)
		return nil, err
	}
	al := &AttributeList{
		iterator: iterator,
	}
	return al, nil
}

// Next returns the next extended attribute. This returns an error when there are no more attributes.
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

// Close closes the iterator. The iterator should not be used after this.
func (i *AttributeList) Close() {
	C.rados_getxattrs_end(i.iterator)
}

// Attribute returns an extended attribute of the object.
func (o *Object) Attribute(attributeName string) (io.Reader, error) {
	object := C.CString(o.name)
	attribute := C.CString(attributeName)
	defer freeString(object)
	defer freeString(attribute)

	bufLen := 2048
	bufAddr := bufferAddress(bufLen)

	ret := C.rados_getxattr(o.ioContext, object, attribute, bufAddr, C.size_t(bufLen))
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to get attribute %s of object %s.", attributeName, o.name)
		return nil, err
	}
	attribbuf := bufToReader(bufAddr, ret)
	return attribbuf, nil
}

// SetAttribute sets an extended attribute of the object.
func (o *Object) SetAttribute(attributeName string, attributeValue io.Reader) error {
	object := C.CString(o.name)
	attribute := C.CString(attributeName)
	defer freeString(object)
	defer freeString(attribute)

	bufAddr, bufLen := readerToBuf(attributeValue)

	ret := C.rados_setxattr(o.ioContext, object, attribute, bufAddr, bufLen)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to set attribute %s of object %s.", attributeName, o.name)
		return err
	}
	return nil
}

// RemoveAttribute removes an attribute of the object.
func (o *Object) RemoveAttribute(attributeName string) error {
	object := C.CString(o.name)
	attribute := C.CString(attributeName)
	defer freeString(object)
	defer freeString(attribute)

	ret := C.rados_rmxattr(o.ioContext, object, attribute)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to get attribute %s of object %s.", attributeName, o.name)
		return err
	}
	return nil
}
