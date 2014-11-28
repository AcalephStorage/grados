package grados

/*
#cgo LDFLAGS: -lrados

#include <rados/librados.h>
*/
import "C"

// LibradosOpFlag are librados read/write operation flags.
type LibradosOpFlag int

// CompareAttribute are read/write operation compare operators.
type CompareAttribute int

// CreateMode are write operation object creation flags.
type CreateMode int

// LibradosOperation are general librados read/write operation flags.
type LibradosOperation int

// LibradosLock are object lock flags.
type LibradosLock int

const (
	OperationExclusive LibradosOpFlag = C.LIBRADOS_OP_FLAG_EXCL   // Fails a create operation if the object already exists.
	OperationFailOk    LibradosOpFlag = C.LIBRADOS_OP_FLAG_FAILOK // Allows the transaction to succeed even if the flagged operation fails.

	Equal            CompareAttribute = C.LIBRADOS_CMPXATTR_OP_EQ  // == comparaison operator.
	NotEqual         CompareAttribute = C.LIBRADOS_CMPXATTR_OP_NE  // != comparison operator.
	GreaterThan      CompareAttribute = C.LIBRADOS_CMPXATTR_OP_GT  // > comparison operator.
	GreaterThanEqual CompareAttribute = C.LIBRADOS_CMPXATTR_OP_GTE // >= comparison operaor.
	LessThan         CompareAttribute = C.LIBRADOS_CMPXATTR_OP_LT  // < comparison operator.
	LessThanEqual    CompareAttribute = C.LIBRADOS_CMPXATTR_OP_LTE // <= comparison operator.

	CreateExclusive  CreateMode = C.LIBRADOS_CREATE_EXCLUSIVE  // Fails a create operation if the object already exists.
	CreateIdempotent CreateMode = C.LIBRADOS_CREATE_IDEMPOTENT // Does not fail a create operation if the object exists.

	NoFlag          LibradosOperation = C.LIBRADOS_OPERATION_NOFLAG             // General Librados Flag. Not much detail in Librados API.
	BalanceReads    LibradosOperation = C.LIBRADOS_OPERATION_BALANCE_READS      // General Librados Flag. Not much detail in Librados API.
	LocalizeReads   LibradosOperation = C.LIBRADOS_OPERATION_LOCALIZE_READS     // General Librados Flag. Not much detail in Librados API.
	OrderReadWrites LibradosOperation = C.LIBRADOS_OPERATION_ORDER_READS_WRITES // General Librados Flag. Not much detail in Librados API.
	IgnoreCache     LibradosOperation = C.LIBRADOS_OPERATION_IGNORE_CACHE       // General Librados Flag. Not much detail in Librados API.
	SkipRWLocks     LibradosOperation = C.LIBRADOS_OPERATION_SKIPRWLOCKS        // General Librados Flag. Not much detail in Librados API.
	IgnoreOverlay   LibradosOperation = C.LIBRADOS_OPERATION_IGNORE_OVERLAY     // General Librados Flag. Not much detail in Librados API.

	NoSnapshot = C.LIBRADOS_SNAP_HEAD // Use this to disable snapshop selection when performing object operations.

	Renew LibradosLock = C.LIBRADOS_LOCK_FLAG_RENEW // Lock Flag. Not much detail in Librados API.
)
