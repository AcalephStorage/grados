package grados

/*
#cgo LDFLAGS: -lrados
#include <rados/librados.h>
*/
import "C"

import (
	"fmt"
	"syscall"
)

// ClusterStatus represents the status of the cluster.
type ClusterStatus struct {
	Total   uint64 // Total storage space of the cluster in KiloBytes.
	Used    uint64 // Used space of the cluster in KiloBytes.
	Free    uint64 // Free space of the cluster in KiloBytes.
	Objects uint64 // Number of objects in the cluster.
}

// Pretty form of ClusterStatus
func (cs *ClusterStatus) String() string {
	return fmt.Sprintf("Total: %d kB, Used: %d kB, Free: %d kB, Objects: %d", cs.Total, cs.Used, cs.Free, cs.Objects)
}

// Status returns the ClusterStatus
func (cluster *Cluster) Status() (*ClusterStatus, error) {
	var clusterStat C.struct_rados_cluster_stat_t
	ret := C.rados_cluster_stat(cluster.handle, &clusterStat)
	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to get cluster status."
		return nil, err
	}
	status := &ClusterStatus{
		Total:   uint64(clusterStat.kb),
		Used:    uint64(clusterStat.kb_used),
		Free:    uint64(clusterStat.kb_avail),
		Objects: uint64(clusterStat.num_objects),
	}
	return status, nil
}

func (cluster *Cluster) Config() *ClusterConfig {
	context := C.rados_cct(cluster.handle)
	return &ClusterConfig{context}
}

// FSID returns the FSID of the cluster.
func (cluster *Cluster) FSID() string {
	bufLen := 37
	for {
		bufAddr := bufferAddress(bufLen)
		ret := C.rados_cluster_fsid(cluster.handle, bufAddr, C.size_t(bufLen))
		if int(ret) == -int(syscall.ERANGE) {
			bufLen *= 2
			continue
		}
		fsid := C.GoStringN(bufAddr, ret)
		return fsid
	}

}

// InstanceId returns the instance ID of the current connection.
func (cluster *Cluster) InstanceId() uint64 {
	return uint64(C.rados_get_instance_id(cluster.handle))
}

// GetConfig returns the configuration value of the given configName.
func (cluster *Cluster) GetConfigValue(configName string) (string, error) {
	cn := C.CString(configName)
	defer freeString(cn)
	bufLen := 8
	for {
		bufAddr := bufferAddress(bufLen)
		ret := C.rados_conf_get(cluster.handle, cn, bufAddr, C.size_t(bufLen))
		if int(ret) == -int(syscall.ENAMETOOLONG) {
			bufLen *= 2
			continue
		}
		if ret < 0 {
			err := toRadosError(ret)
			err.Message = fmt.Sprintf("Unable to get config value of %s.", configName)
			return "", err
		}
		value := C.GoString(bufAddr)
		return value, nil
	}
}

// WaitForLatestOsdMap blocks until the latest OSD Map is ready
func (cluster *Cluster) WaitForLatestOsdMap() error {
	ret := C.rados_wait_for_latest_osdmap(cluster.handle)
	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to wait for OSD Map"
		return err
	}
	return nil
}
