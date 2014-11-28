package grados

/*
#cgo LDFLAGS: -lrados
#include <rados/librados.h>
*/
import "C"

import (
	"fmt"
	"io"
	"syscall"
	"time"
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
	defer freeString(k)
	C.rados_ioctx_locator_set_key(pool.context, k)
}

// Set the namespace for objects.
//
// The namespace specification further refines a pool into different domains. The mapping of objects to pgs is also
// based on this value.
func (pool *Pool) SetNamespace(namespace string) {
	n := C.CString(namespace)
	defer freeString(n)
	C.rados_ioctx_set_namespace(pool.context, n)
}

// LastObjectVersion returns the version of the last object read or written.
func (pool *Pool) LastObjectVersion() uint64 {
	return uint64(C.rados_get_last_version(pool.context))
}

type Object struct {
	ioContext   C.rados_ioctx_t
	name        string
	watchHandle C.uint64_t
}

func (pool *Pool) ManageObject(name string) *Object {
	return &Object{
		ioContext: pool.context,
		name:      name,
	}
}

// Write writes the data at a specific offset to the object.
func (o *Object) Write(data io.Reader, offset uint64) error {
	oid := C.CString(o.name)
	defer freeString(oid)
	bufAddr, length := readerToBuf(data)
	ret := C.rados_write(o.ioContext, oid, bufAddr, length, C.uint64_t(offset))
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to write data to object %s", o.name)
		return err
	}
	return nil
}

// WriteFull writes the entire data to the object replacing old data.
func (o *Object) WriteFull(data io.Reader) error {
	oid := C.CString(o.name)
	defer freeString(oid)
	bufAddr, length := readerToBuf(data)
	ret := C.rados_write_full(o.ioContext, oid, bufAddr, length)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to write full data to object %s", o.name)
		return err
	}
	return nil
}

// Append appends new data to the object
func (o *Object) Append(data io.Reader) error {
	oid := C.CString(o.name)
	defer freeString(oid)
	bufAddr, length := readerToBuf(data)
	ret := C.rados_append(o.ioContext, oid, bufAddr, length)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to append data to object %s", o.name)
		return err
	}
	return nil
}

// Read reads a specified length of data from the object starting at the given offset.
func (o *Object) Read(length, offset uint64) (io.Reader, error) {
	oid := C.CString(o.name)
	defer freeString(oid)
	bufAddr := bufferAddress(int(length))
	ret := C.rados_read(o.ioContext, oid, bufAddr, C.size_t(length), C.uint64_t(offset))
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to read object %s.", o.name)
		return nil, err
	}
	return bufToReader(bufAddr, ret), nil
}

// Remove removes the object from the pool.
func (o *Object) Remove() error {
	oid := C.CString(o.name)
	defer freeString(oid)
	ret := C.rados_remove(o.ioContext, oid)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to delete object %s", o.name)
		return err
	}
	return nil
}

// Truncate modifies the size of an object. If the object size in increased, the new space is zero-filled. If the size
// is reduced, the excess data is removed.
func (o *Object) Truncate(size uint64) error {
	oid := C.CString(o.name)
	defer freeString(oid)
	s := C.uint64_t(size)
	ret := C.rados_trunc(o.ioContext, oid, s)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to resize object %s to size %d.", o.name, size)
		return err
	}
	return nil
}

// Clone clones a length of data from an object given an offest to another object starting at an offset. The source
// and destination objects must be on the same PG. This requires that a locator key must be set first.
func (o *Object) Clone(target *Object, srcOffset, dstOffset, length uint64) error {
	srcOid := C.CString(o.name)
	dstOid := C.CString(target.name)

	defer freeString(srcOid)
	defer freeString(dstOid)

	so := C.uint64_t(srcOffset)
	do := C.uint64_t(dstOffset)

	ln := C.size_t(length)

	ret := C.rados_clone_range(o.ioContext, dstOid, do, srcOid, so, ln)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to clone %s to %s.", o.name, target.name)
		return err
	}
	return nil
}

type ObjectStatus struct {
	size         uint64
	modifiedTime time.Time
}

func (o *Object) Status() (*ObjectStatus, error) {
	oid := C.CString(o.name)
	defer freeString(oid)

	var objectSize C.uint64_t
	var modifiedTime C.time_t

	ret := C.rados_stat(o.ioContext, oid, &objectSize, &modifiedTime)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to get status for object %s.", o.name)
		return nil, err
	}
	return &ObjectStatus{
		size:         uint64(objectSize),
		modifiedTime: time.Unix(int64(modifiedTime), 0),
	}, nil
}

func (o *Object) SetAllocationHint(expectedObjectSize, expectedWriteSize uint64) {
	oid := C.CString(o.name)
	defer freeString(oid)
	es := C.uint64_t(expectedObjectSize)
	ews := C.uint64_t(expectedWriteSize)
	C.rados_set_alloc_hint(o.ioContext, oid, es, ews)
}

// TODO: add lock duration
func (o *Object) LockExclusive(name, cookie, description string, flags ...LibradosLock) error {
	oid := C.CString(o.name)
	defer freeString(oid)

	n := C.CString(name)
	defer freeString(n)

	c := C.CString(cookie)
	defer freeString(c)

	d := C.CString(description)
	defer freeString(d)

	f := 0
	for _, flag := range flags {
		f |= int(flag)
	}
	ret := C.rados_lock_exclusive(o.ioContext, oid, n, c, d, nil, C.uint8_t(f))
	switch int(ret) {
	case -int(syscall.EBUSY):
		err := toRadosError(ret)
		err.Message = fmt.Sprintf("%s is already locked by another client", o.name)
		return err
	case -int(syscall.EEXIST):
		err := toRadosError(ret)
		err.Message = fmt.Sprintf("%s is already locked by current client", o.name)
		return err
	}
	return nil
}

// TODO: add lock duration
func (o *Object) LockShared(name, cookie, tag, description string, flags ...LibradosLock) error {
	oid := C.CString(o.name)
	defer freeString(oid)

	n := C.CString(name)
	defer freeString(n)

	c := C.CString(cookie)
	defer freeString(c)

	t := C.CString(tag)
	defer freeString(t)

	d := C.CString(description)
	defer freeString(d)

	f := 0
	for _, flag := range flags {
		f |= int(flag)
	}

	ret := C.rados_lock_shared(o.ioContext, oid, n, c, t, d, nil, C.uint8_t(f))
	switch int(ret) {
	case -int(syscall.EBUSY):
		err := toRadosError(ret)
		err.Message = fmt.Sprintf("%s is already locked by another client", o.name)
		return err
	case -int(syscall.EEXIST):
		err := toRadosError(ret)
		err.Message = fmt.Sprintf("%s is already locked by current client", o.name)
		return err
	}
	return nil
}

func (o *Object) Unlock(name, cookie string) error {
	oid := C.CString(o.name)
	defer freeString(oid)

	n := C.CString(name)
	defer freeString(n)

	c := C.CString(cookie)
	defer freeString(c)

	ret := C.rados_unlock(o.ioContext, oid, n, c)
	if int(ret) == -int(syscall.ENOENT) {
		err := toRadosError(ret)
		err.Message = fmt.Sprintf("%s does not own the lock.", o.name)
		return err
	}
	return nil
}

func (o *Object) BreakLock(name, client, cookie string) error {
	oid := C.CString(o.name)
	defer freeString(oid)

	n := C.CString(name)
	defer freeString(n)

	cl := C.CString(client)
	defer freeString(cl)

	c := C.CString(cookie)
	defer freeString(c)

	ret := C.rados_break_lock(o.ioContext, oid, n, cl, c)
	switch int(ret) {
	case -int(syscall.ENOENT):
		err := toRadosError(ret)
		err.Message = fmt.Sprintf("%s lock is not held by %s:%s", o.name, client, cookie)
		return err
	case -int(syscall.EINVAL):
		err := toRadosError(ret)
		err.Message = fmt.Sprintf("%s client cannot be parsed.", client)
		return err
	}
	return nil
}

type Locker struct {
	Client  string
	Cookies string
	Address string
}

func (o *Object) ListLockers(name string) ([]*Locker, string, error) {
	oid := C.CString(o.name)
	defer freeString(oid)

	n := C.CString(name)
	defer freeString(n)

	var exclusive C.int

	bufLen := 2048
	var tagLen C.size_t
	var clientsLen C.size_t
	var cookiesLen C.size_t
	var addrsLen C.size_t

	for {
		cTag := bufferAddress(bufLen)
		cClients := bufferAddress(bufLen)
		cCookies := bufferAddress(bufLen)
		cAddrs := bufferAddress(bufLen)

		ret := C.rados_list_lockers(o.ioContext, oid, n, &exclusive, cTag, &tagLen, cClients, &clientsLen, cCookies, &cookiesLen, cAddrs, &addrsLen)
		if int(ret) == -int(syscall.ERANGE) {
			bufLen *= 2
			continue
		}
		if err := toRadosError(C.int(ret)); err != nil {
			err.Message = fmt.Sprintf("Unable to get lockers for object %s.", o.name)
			return nil, "", err
		}

		tag := C.GoStringN(cTag, C.int(tagLen))

		clients := bufToStringSlice(cClients, C.int(clientsLen))
		cookies := bufToStringSlice(cCookies, C.int(cookiesLen))
		addrs := bufToStringSlice(cAddrs, C.int(addrsLen))

		lockers := make([]*Locker, ret)
		for i := 0; i < int(ret); i++ {
			lockers[i] = &Locker{
				Client:  clients[i],
				Cookies: cookies[i],
				Address: addrs[i],
			}
		}

		return lockers, tag, nil
	}

}
