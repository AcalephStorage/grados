package grados

// /*
// #cgo LDFLAGS: -lrados
// #include <rados/librados.h>
// */
// import "C"

// func (o *Object) Watch(version uint64) {
// 	oid := C.CString(o.Name)
// 	defer freeString(oid)
// 	ret := C.rados_watch(o.ioContext, iod, C.uint64_t(version), uint64_t *handle, rados_watchcb_t watchcb, void *arg)

// }

// func objectChangeCallback(opCode C.uint8_t, )

// // void (*rados_watchcb_t)(uint8_t opcode, uint64_t ver, void *arg);
