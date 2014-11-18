package grados

/*
#cgo LDFLAGS: -lrados
#include <stdlib.h>
#include <rados/librados.h>
*/
import "C"

import (
	"bytes"
	"fmt"
	"unsafe"
)

// PoolStatus represents the status of an induividual pool.
type PoolStatus struct {
	FreeBytes               uint64 // Free space in bytes.
	FreeKiloBytes           uint64 // Free space in kilobytes.
	Objects                 uint64 // number of objects.
	ObjectClones            uint64 // number of object clones.
	ObjectCopies            uint64 // number of object copies.
	ObjectsMissingOnPrimary uint64 // number of objects missing on primary.
	ObjectsFound            uint64 // number of objects found.
	ObjectsDegraded         uint64 // number of objects degraded.
	ReadBytes               uint64 // read bytes.
	ReadKiloBytes           uint64 // read bytes in kilobytes.
	WriteBytes              uint64 // written bytes.
	WriteKiloBytes          uint64 // written bytes in kilobytes.
}

// Pretty PoolStatus
func (ps *PoolStatus) String() string {
	return fmt.Sprintf("Free: %d bytes | %d kB, Objects: %d, Clones: %d, Copies: %d, MissingOnPrimary: %d, Found: %d, Degraded: %d, Read: %d bytes | %d kB, Write %d bytes | %d kB",
		ps.FreeBytes,
		ps.FreeKiloBytes,
		ps.Objects,
		ps.ObjectClones,
		ps.ObjectCopies,
		ps.ObjectsMissingOnPrimary,
		ps.ObjectsFound,
		ps.ObjectsDegraded,
		ps.ReadBytes,
		ps.ReadKiloBytes,
		ps.WriteBytes,
		ps.WriteKiloBytes,
	)
}

// Pool represents a pool io context. This contains pool related functions.
type Pool struct {
	context C.rados_ioctx_t
}

// OpenPool opens a pool for query, read, and write operations.
func (cluster *Cluster) OpenPool(poolName string) (*Pool, error) {
	p := C.CString(poolName)
	defer C.free(unsafe.Pointer(p))
	var ioContext C.rados_ioctx_t
	ret := C.rados_ioctx_create(cluster.handle, p, &ioContext)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to create IO Context for %s.", poolName)
		return nil, err
	}
	return &Pool{ioContext}, nil
}

// CloseWhenDone this pool context when all asynchronous writes are done.
func (pool *Pool) CloseWhenDone() {
	C.rados_aio_flush(pool.context)
	pool.CloseNow()
}

// CloseNow this pool context immediately.
func (pool *Pool) CloseNow() {
	C.rados_ioctx_destroy(pool.context)
}

// Config returns a reference of the ClusterConfig.
func (pool *Pool) Config() *ClusterConfig {
	config := new(ClusterConfig)
	config.context = C.rados_ioctx_cct(pool.context)
	return config
}

// Cluster returns a reference of the Cluster handle.
func (pool *Pool) Cluster() *Cluster {
	cluster := new(Cluster)
	cluster.handle = C.rados_ioctx_get_cluster(pool.context)
	return cluster
}

// Status retrieves the PoolStatus.
func (pool *Pool) Status() (*PoolStatus, error) {
	var poolStat C.struct_rados_pool_stat_t
	ret := C.rados_ioctx_pool_stat(pool.context, &poolStat)
	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to get pool status."
		return nil, err
	}

	status := &PoolStatus{
		FreeBytes:               uint64(poolStat.num_bytes),
		FreeKiloBytes:           uint64(poolStat.num_kb),
		Objects:                 uint64(poolStat.num_objects),
		ObjectClones:            uint64(poolStat.num_object_clones),
		ObjectCopies:            uint64(poolStat.num_object_copies),
		ObjectsMissingOnPrimary: uint64(poolStat.num_objects_missing_on_primary),
		ObjectsFound:            uint64(poolStat.num_objects_unfound),
		ObjectsDegraded:         uint64(poolStat.num_objects_degraded),
		ReadBytes:               uint64(poolStat.num_rd),
		ReadKiloBytes:           uint64(poolStat.num_rd_kb),
		WriteBytes:              uint64(poolStat.num_wr),
		WriteKiloBytes:          uint64(poolStat.num_wr_kb),
	}
	return status, nil
}

// SetAUID attempts to change the AUID for the pool.
func (pool *Pool) SetAUID(auid uint64) error {
	ret := C.rados_ioctx_pool_set_auid(pool.context, C.uint64_t(auid))
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to set auid to %d", auid)
		return err
	}
	return nil
}

// AUID returns the AUID of the pool.
func (pool *Pool) AUID() (uint64, error) {
	var auid C.uint64_t
	ret := C.rados_ioctx_pool_get_auid(pool.context, &auid)
	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to retrieve AUID"
		return 0, err
	}
	return uint64(auid), nil
}

// Id returns the id of the pool.
func (pool *Pool) Id() int64 {
	return int64(C.rados_ioctx_get_id(pool.context))
}

// Name returns the name of the pool.
func (pool *Pool) Name() string {
	buf := make([]byte, 64)
	for {
		bufAddr := (*C.char)(unsafe.Pointer(&buf[0]))
		ret := C.rados_ioctx_get_pool_name(pool.context, bufAddr, C.unsigned(len(buf)))
		if ret == errorRange {
			buf = make([]byte, len(buf)*2)
			continue
		}
		return C.GoStringN(bufAddr, ret)
	}
}

// RequiresAlignment returns true if the pool requires alignment.
func (pool *Pool) RequiresAlignment() bool {
	ret := C.rados_ioctx_pool_requires_alignment(pool.context)
	return ret != 0
}

// TODO:
func (pool *Pool) RequiredAlignment() uint64 {
	return uint64(C.rados_ioctx_pool_required_alignment(pool.context))
}

// CreateSnapshot creates a pool wide snapshot.
func (pool *Pool) CreateSnapshot(snapshotName string) error {
	name := C.CString(snapshotName)
	defer C.free(unsafe.Pointer(name))
	ret := C.rados_ioctx_snap_create(pool.context, name)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to create snapshot %s", snapshotName)
		return err
	}
	return nil
}

// RemoveSnapshot removes a pool wide snapshot.
func (pool *Pool) RemoveSnapshot(snapshotName string) error {
	name := C.CString(snapshotName)
	defer C.free(unsafe.Pointer(name))
	ret := C.rados_ioctx_snap_remove(pool.context, name)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to remove snapshot %s", snapshotName)
		return err
	}
	return nil
}

// RollbackSnapshot rolls back an object to a pool snapshot.
func (pool *Pool) RollbackSnapshot(objectName, snapshotName string) error {
	object := C.CString(objectName)
	snapshot := C.CString(snapshotName)
	defer C.free(unsafe.Pointer(object))
	defer C.free(unsafe.Pointer(snapshot))
	ret := C.rados_ioctx_snap_rollback(pool.context, object, snapshot)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to rollback object %s to snapshot %s", objectName, snapshotName)
		return err
	}
	return nil
}

// ListPools returns all the pools in the ceph cluster.
func (cluster *Cluster) ListPools() ([]string, error) {
	buf := make([]byte, 4096)
	pools := make([]string, 0)
	for {
		bufAddr := (*C.char)(unsafe.Pointer(&buf[0]))
		ret := C.rados_pool_list(cluster.handle, bufAddr, C.size_t(len(buf)))
		if ret < 0 {
			err := toRadosError(ret)
			err.Message = "Unable to retrieve pool list"
			return nil, err
		}

		if int(ret) > len(buf) {
			buf = make([]byte, ret)
			continue
		}

		tmp := bytes.SplitAfter(buf[:ret-1], []byte{0})
		for _, s := range tmp {
			if len(s) > 0 {
				pool := C.GoString(bufAddr)
				pools = append(pools, pool)
			}
		}

		return pools, nil
	}
}

// LookupPool returns the pool ID of the given pool name.
func (cluster *Cluster) LookupPool(poolName string) (int, error) {
	p := C.CString(poolName)
	defer C.free(unsafe.Pointer(p))
	poolId := C.rados_pool_lookup(cluster.handle, p)
	if err := toRadosError(C.int(poolId)); err != nil {
		err.Message = fmt.Sprintf("Unable to lookup pool ID for %s.", poolName)
		return -1, err
	}
	return int(poolId), nil
}

// ReverseLookupPool returns the pool name of the given pool ID.
func (cluster *Cluster) ReverseLookupPool(id int) (string, error) {
	buf := make([]byte, 64)
	for {
		bufAddr := (*C.char)(unsafe.Pointer(&buf[0]))
		ret := C.rados_pool_reverse_lookup(cluster.handle, C.int64_t(id), bufAddr, C.size_t(len(buf)))
		if ret == errorRange {
			buf = make([]byte, len(buf)*2)
			continue
		} else if ret < 0 {
			err := toRadosError(ret)
			err.Message = fmt.Sprintf("Unable to retrieve pool name for %d.", id)
			return "", err
		}
		poolName := C.GoStringN(bufAddr, ret)
		return poolName, nil
	}
}

// CreatePool creates a new pool using the given poolName. This uses the default pool configuration.
func (cluster *Cluster) CreatePool(poolName string) error {
	p := C.CString(poolName)
	defer C.free(unsafe.Pointer(p))
	ret := C.rados_pool_create(cluster.handle, p)
	err := toRadosError(ret)
	if err != nil {
		err.Message = fmt.Sprintf("Unable to create pool %s with default settings.", poolName)
		return err
	}
	return nil
}

// CreatePoolWithOwner creates a new pool using the given poolName and sets it auid.
func (cluster *Cluster) CreatePoolWithOwner(poolName string, auid uint64) error {
	p := C.CString(poolName)
	defer C.free(unsafe.Pointer(p))
	ret := C.rados_pool_create_with_auid(cluster.handle, p, C.uint64_t(auid))
	err := toRadosError(ret)
	if err != nil {
		err.Message = fmt.Sprintf("Unable to create pool %s with auid %d.", poolName, auid)
		return err
	}
	return nil
}

// CreatePoolWithCrushRule creates a new pool using the given poolName with a crushRule set.
func (cluster *Cluster) CreatePoolWithCrushRule(poolName string, crushRule uint8) error {
	p := C.CString(poolName)
	defer C.free(unsafe.Pointer(p))
	ret := C.rados_pool_create_with_crush_rule(cluster.handle, p, C.uint8_t(crushRule))
	err := toRadosError(ret)
	if err != nil {
		err.Message = fmt.Sprintf("Unable to create pool %s with crush rule %d.", poolName, crushRule)
		return err
	}
	return nil
}

// CreatePoolWithAll creates a new pool using the given poolName with both auid and crushRule set.
func (cluster *Cluster) CreatePoolWithAll(poolName string, auid uint64, crushRule uint8) error {
	p := C.CString(poolName)
	defer C.free(unsafe.Pointer(p))
	ret := C.rados_pool_create_with_all(cluster.handle, p, C.uint64_t(auid), C.uint8_t(crushRule))
	err := toRadosError(ret)
	if err != nil {
		err.Message = fmt.Sprintf("Unable to create pool %s with auid %d and crush rule %d.", poolName, auid, crushRule)
		return err
	}
	return nil
}

// DeletePool removes a pool from the cluster.
func (cluster *Cluster) DeletePool(poolName string) error {
	p := C.CString(poolName)
	defer C.free(unsafe.Pointer(p))
	ret := C.rados_pool_delete(cluster.handle, p)
	err := toRadosError(ret)
	if err != nil {
		err.Message = fmt.Sprintf("Unable to delete %s pool.", poolName)
		return err
	}
	return nil
}