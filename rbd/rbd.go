package rbd

/*
#cgo LDFLAGS: -lrbd
#include <rbd/librbd.h>
*/
import "C"

import (
	"fmt"
)

// Version will return current librbd version.
func Version() string {
	var major, minor, extra C.int
	C.rbd_version(&major, &minor, &extra)
	return fmt.Sprintf("v%d.%d.%d", major, minor, extra)
}
