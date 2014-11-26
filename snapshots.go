package grados

/*
#cgo LDFLAGS: -lrados
#include <rados/librados.h>
*/
import "C"

import (
	"fmt"
	"sort"
	"syscall"
	"time"
)

const (
	NoSnapshot = C.LIBRADOS_SNAP_HEAD
)

type SnapshotId uint64

// ManagedSnapshot is a reference to a self managed snapshot Id.
type ManagedSnapshot struct {
	Id        SnapshotId
	ioContext C.rados_ioctx_t
}

// Snapshots represents a list of Snapshot reference
type ManagedSnapshots []*ManagedSnapshot

// Len implements sort.Interface for sorting. Returns the number of snaphots.
func (s ManagedSnapshots) Len() int {
	return len(s)
}

// Less implements sort.Interface for sorting. Returns true if index i is less than index j.
func (s ManagedSnapshots) Less(i, j int) bool {
	return s[i].Id <= s[j].Id
}

// Swap implements sort.Interface for sorting. Swaps the values of index i and j.
func (s ManagedSnapshots) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// CreateSelfManagedSnapshot creates an id for reference to manipulating self managed snapshots. A clone will not be
// created until a write using the created snapshot context is done.
func (pool *Pool) CreateSelfManagedSnapshot() (*ManagedSnapshot, error) {
	snapshot := &ManagedSnapshot{
		ioContext: pool.context,
	}
	ret := C.rados_ioctx_selfmanaged_snap_create(pool.context, (*C.rados_snap_t)(&snapshot.Id))
	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to create self managed snapshot."
		return nil, err
	}
	return snapshot, nil
}

// SetAsWriteContext sets all new writes will use this snapshot when writing. // TODO: Needs better testing
func (snapshot *ManagedSnapshot) SetAsWriteContext(snapshots ManagedSnapshots) error {
	sort.Sort(sort.Reverse(snapshots))
	snapContexts := make([]C.rados_snap_t, len(snapshots))
	for i, s := range snapshots {
		snapContexts[i] = C.rados_snap_t(s.Id)
	}
	seq := C.rados_snap_t(snapshot.Id)
	maxLen := C.int(len(snapshots))
	ret := C.rados_ioctx_selfmanaged_snap_set_write_ctx(snapshot.ioContext, seq, &snapContexts[0], maxLen)
	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to set snapshot context for writing objects."
		return err
	}
	return nil
}

// Rollback will rollback an object to this snapshot. TODO: Needs testing
func (snapshot *ManagedSnapshot) Rollback(objectId string) error {
	oid := C.CString(objectId)
	defer freeString(oid)
	ret := C.rados_ioctx_selfmanaged_snap_rollback(snapshot.ioContext, oid, C.rados_snap_t(snapshot.Id))
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to rollback %s to managed pool %d.", objectId, snapshot.Id)
		return err
	}
	return nil
}

// Remove will lazily remove the self managed snapshot.
func (snapshot *ManagedSnapshot) Remove() error {
	ret := C.rados_ioctx_selfmanaged_snap_remove(snapshot.ioContext, C.rados_snap_t(snapshot.Id))
	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to remove snapshot."
		return err
	}
	return nil
}

// ListSelfManagedSnapshots returns a list of all self managed snapshots.
func (pool *Pool) ListPoolSnapshots() []SnapshotId {
	snaps := make([]SnapshotId, 0)
	for {
		var addr *C.rados_snap_t
		if len(snaps) > 0 {
			addr = (*C.rados_snap_t)(&snaps[0])
		}
		ret := C.rados_ioctx_snap_list(pool.context, addr, C.int(len(snaps)))
		if int(ret) == -int(syscall.ERANGE) {
			snaps = make([]SnapshotId, C.int(len(snaps))+1)
			continue
		}
		return snaps
	}
}

// CreateSnapshot creates a pool wide snapshot.
func (pool *Pool) CreatePoolSnapshot(snapshotName string) error {
	name := C.CString(snapshotName)
	defer freeString(name)
	ret := C.rados_ioctx_snap_create(pool.context, name)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to create snapshot %s", snapshotName)
		return err
	}
	return nil
}

// RemoveSnapshot removes a pool wide snapshot.
func (pool *Pool) RemovePoolSnapshot(snapshotName string) error {
	name := C.CString(snapshotName)
	defer freeString(name)
	ret := C.rados_ioctx_snap_remove(pool.context, name)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to remove snapshot %s", snapshotName)
		return err
	}
	return nil
}

// RollbackSnapshot rolls back an object to a pool snapshot.
func (pool *Pool) RollbackToPoolSnapshot(objectName, snapshotName string) error {
	object := C.CString(objectName)
	snapshot := C.CString(snapshotName)
	defer freeString(object)
	defer freeString(snapshot)
	ret := C.rados_ioctx_snap_rollback(pool.context, object, snapshot)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to rollback object %s to snapshot %s", objectName, snapshotName)
		return err
	}
	return nil
}

// SnapshotLookup returns the id of the given snapshot.
func (pool *Pool) LookupPoolSnapshot(snapshotName string) (SnapshotId, error) {
	name := C.CString(snapshotName)
	defer freeString(name)
	var id C.rados_snap_t
	ret := C.rados_ioctx_snap_lookup(pool.context, name, &id)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to lookup id for pool snapshot %s.", snapshotName)
		return 0, err
	}
	return SnapshotId(id), nil
}

// ReverseLookupSnapshot returns the name of the given pool snapshot id.
func (pool *Pool) ReverseLookupSnapshot(snapId SnapshotId) (string, error) {
	id := C.rados_snap_t(snapId)
	bufLen := 8
	for {
		bufAddr := bufferAddress(bufLen)
		ret := C.rados_ioctx_snap_get_name(pool.context, id, bufAddr, C.int(bufLen))
		if int(ret) == -int(syscall.ERANGE) {
			bufLen *= 2
			continue
		}
		if ret < 0 {
			err := toRadosError(ret)
			err.Message = fmt.Sprintf("Unable to reverse lookup pool snapshot id %d.", snapId)
			return "", err
		}
		name := C.GoString(bufAddr)
		return name, nil
	}
}

// SnapshotTimestamp returns the timestamp the snapshot was created.
func (pool *Pool) SnapshotTimestamp(snapId SnapshotId) (time.Time, error) {
	id := C.rados_snap_t(snapId)
	var t C.time_t
	ret := C.rados_ioctx_snap_get_stamp(pool.context, id, &t)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to retrieve timestamp for snapshot id %d.", snapId)
		return time.Now(), err
	}
	goTime := time.Unix(int64(t), 0)
	return goTime, nil
}

// UseSnapshot sets the pool context to use the snapshot for successive reads. Use NoSnapshot to not use snapshots when
// reading.
func (pool *Pool) UseSnapshot(snapId SnapshotId) {
	id := C.rados_snap_t(snapId)
	C.rados_ioctx_snap_set_read(pool.context, id)
}
