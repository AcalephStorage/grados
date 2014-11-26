package grados

/*
#include <stdlib.h>
*/
import "C"

import (
	"bytes"
	"io"
	"unsafe"
)

func readerToBuf(data io.Reader) (addr *C.char, length C.size_t) {
	buf := new(bytes.Buffer)
	buf.ReadFrom(data)
	addr = (*C.char)(unsafe.Pointer(&buf.Bytes()[0]))
	length = C.size_t(buf.Len())
	return
}

func bufferAddress(size int) *C.char {
	buf := make([]byte, size)
	bufAddr := (*C.char)(unsafe.Pointer(&buf[0]))
	return bufAddr
}

func bufToReader(buf *C.char, bufLen C.int) io.Reader {
	b := C.GoBytes(unsafe.Pointer(buf), bufLen)
	return bytes.NewBuffer(b)
}

func freeString(str *C.char) {
	C.free(unsafe.Pointer(str))
}
