package grados

/*
#cgo LDFLAGS: -lrados
#include <rados/librados.h>
*/
import "C"

// ObjectList represents a handler for iterating through objects of a pool.
type ObjectList struct {
	ioContext   C.rados_ioctx_t
	listContext C.rados_list_ctx_t
}

// OpenObjectList returns an ObjectList handler to start iterating over objects of a pool.
func (pool *Pool) OpenObjectList() (*ObjectList, error) {
	objectList := new(ObjectList)
	objectList.ioContext = pool.context
	ret := C.rados_objects_list_open(objectList.ioContext, &objectList.listContext)
	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to open object list"
		return nil, err
	}
	return objectList, nil
}

// Position returns the hash of the position rounded to the nearest PG.
func (ol *ObjectList) Position() uint32 {
	ret := C.rados_objects_list_get_pg_hash_position(ol.listContext)
	position := uint32(ret)
	return position
}

// Seek moves the iterator pointer to the given position. This returns the new position rouded to the nearest PG.
func (ol *ObjectList) Seek(position uint32) uint32 {
	newPosition := C.uint32_t(position)
	ret := C.rados_objects_list_seek(ol.listContext, newPosition)
	return uint32(ret)
}

// Next returns the objectId and locationKey (if any) of the next object.
func (ol *ObjectList) Next() (object *Object, locationKey string, err error) {
	var e *C.char
	var k *C.char
	ret := C.rados_objects_list_next(ol.listContext, &e, &k)
	if errs := toRadosError(ret); errs != nil {
		errs.Message = "Unable to get next object from list."
		err = errs
	} else {
		object = &Object{
			ioContext: ol.ioContext,
			Name:      C.GoString(e),
		}
		locationKey = C.GoString(k)
	}
	return
}

// Close closes the iterator.
func (ol *ObjectList) Close() {
	C.rados_objects_list_close(ol.listContext)
}
