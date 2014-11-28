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

// readerToBuffer creates a C buffer from the given reader.
func readerToBuf(data io.Reader) (addr *C.char, length C.size_t) {
	buf := new(bytes.Buffer)
	buf.ReadFrom(data)
	addr = (*C.char)(unsafe.Pointer(&buf.Bytes()[0]))
	length = C.size_t(buf.Len())
	return
}

// bufferAddress creates a C buffer with a given size.
func bufferAddress(size int) *C.char {
	buf := make([]byte, size)
	bufAddr := (*C.char)(unsafe.Pointer(&buf[0]))
	return bufAddr
}

// bufToReader creates an io.Reader from the given C buffer.
func bufToReader(buf *C.char, bufLen C.int) io.Reader {
	b := C.GoBytes(unsafe.Pointer(buf), bufLen)
	return bytes.NewBuffer(b)
}

// freeString frees up memory allocation of a given string. Used with C.CString()
func freeString(str *C.char) {
	C.free(unsafe.Pointer(str))
}

// bufToStringSlice converts a C buffer containing several strings separated by \0 to a string slice.
func bufToStringSlice(bufAddr *C.char, ret C.int) []string {
	reader := bufToReader(bufAddr, ret)
	buf := new(bytes.Buffer)
	buf.ReadFrom(reader)

	result := make([]string, 0)
	tmp := bytes.SplitAfter(buf.Bytes()[:ret-1], []byte{0})
	for _, s := range tmp {
		if len(s) > 0 {
			result = append(result, string(s))
		}
	}
	return result
}
