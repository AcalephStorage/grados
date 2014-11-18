package grados

/*
#cgo LDFLAGS: -lrados

#include <stdlib.h>
#include <rados/librados.h>
*/
import "C"

// ObjectList represents a handler for iterating through objects of a pool.
type ObjectList struct {
	context C.rados_list_ctx_t
}

// OpenObjectList returns an ObjectList handler to start iterating over objects of a pool.
func (pool *Pool) OpenObjectList() (*ObjectList, error) {
	objectList := new(ObjectList)
	ret := C.rados_objects_list_open(pool.context, &objectList.context)
	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to open object list"
		return nil, err
	}
	return objectList, nil
}

// Position returns the hash of the position rounded to the nearest PG.
func (objectList *ObjectList) Position() uint32 {
	ret := C.rados_objects_list_get_pg_hash_position(objectList.context)
	position := uint32(ret)
	return position
}

// Seek moves the iterator pointer to the given position. This returns the new position rouded to the nearest PG.
func (objectList *ObjectList) Seek(position uint32) uint32 {
	newPosition := C.uint32_t(position)
	ret := C.rados_objects_list_seek(objectList.context, newPosition)
	return uint32(ret)
}

// Next returns the entry (name of the object), key (location key if any) of the next object.
func (objectList *ObjectList) Next() (entry, key string, err error) {
	var e *C.char
	var k *C.char
	ret := C.rados_objects_list_next(objectList.context, &e, &k)
	if errs := toRadosError(ret); errs != nil {
		errs.Message = "Unable to get next object from list."
		err = errs
	} else {
		entry = C.GoString(e)
		key = C.GoString(k)
	}
	return
}

// Close closes the iterator.
func (objectList *ObjectList) Close() {
	C.rados_objects_list_close(objectList.context)
}
