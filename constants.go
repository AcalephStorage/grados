package grados

/*
#cgo LDFLAGS: -lrados

#include <rados/librados.h>
*/
import "C"

type LibradosOpFlag int
type CompareAttribute int
type CreateMode int
type LibradosOperation int
type LibradosLock int

const (
	OperationExclusive LibradosOpFlag = C.LIBRADOS_OP_FLAG_EXCL
	OperationFailOk    LibradosOpFlag = C.LIBRADOS_OP_FLAG_FAILOK

	Equal            CompareAttribute = C.LIBRADOS_CMPXATTR_OP_EQ
	NotEqual         CompareAttribute = C.LIBRADOS_CMPXATTR_OP_NE
	GreaterThan      CompareAttribute = C.LIBRADOS_CMPXATTR_OP_GT
	GreaterThanEqual CompareAttribute = C.LIBRADOS_CMPXATTR_OP_GTE
	LessThan         CompareAttribute = C.LIBRADOS_CMPXATTR_OP_LT
	LessThanEqual    CompareAttribute = C.LIBRADOS_CMPXATTR_OP_LTE

	CreateExclusive  CreateMode = C.LIBRADOS_CREATE_EXCLUSIVE
	CreateIdempotent CreateMode = C.LIBRADOS_CREATE_IDEMPOTENT

	NoFlag          LibradosOperation = C.LIBRADOS_OPERATION_NOFLAG
	BalanceReads    LibradosOperation = C.LIBRADOS_OPERATION_BALANCE_READS
	LocalizeReads   LibradosOperation = C.LIBRADOS_OPERATION_LOCALIZE_READS
	OrderReadWrites LibradosOperation = C.LIBRADOS_OPERATION_ORDER_READS_WRITES
	IgnoreCache     LibradosOperation = C.LIBRADOS_OPERATION_IGNORE_CACHE
	SkipRWLocks     LibradosOperation = C.LIBRADOS_OPERATION_SKIPRWLOCKS
	IgnoreOverlay   LibradosOperation = C.LIBRADOS_OPERATION_IGNORE_OVERLAY

	Renew LibradosLock = C.LIBRADOS_LOCK_FLAG_RENEW
)
