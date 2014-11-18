package grados

import "testing"

func TestIterateObjects(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	pool, err := cluster.OpenPool("data")
	handleError(t, err)
	if pool == nil {
		return
	}
	objList, err := pool.OpenObjectList()
	handleError(t, err)
	if objList == nil {
		return
	}
	// for ; err != nil; {
	entry, key, err := objList.Next()
	p := objList.Position()
	t.Log("object: ", p, entry, key)
	entry, key, err = objList.Next()
	p = objList.Position()
	t.Log("object: ", p, entry, key)
	// }
	objList.Close()
}
