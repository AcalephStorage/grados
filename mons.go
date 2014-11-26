package grados

/*
#cgo LDFLAGS: -lrados
#include <rados/librados.h>
*/
import "C"

// PingMonitor will query the given monitor to check it's status
// TODO: Make struct for result
func (cluster *Cluster) PingMonitor(monitorId string) (string, error) {
	monId := C.CString(monitorId)
	defer freeString(monId)

	var outLen C.size_t
	var out *C.char

	ret := C.rados_ping_monitor(cluster.handle, monId, &out, &outLen)
	defer C.rados_buffer_free(out)

	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to ping monitor"
		return "", err
	}

	result := C.GoStringN(out, (C.int)(outLen))
	return result, nil
}
