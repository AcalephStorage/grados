package grados

import "testing"
import "bytes"

func TestIterateObjects(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}

	if err := cluster.CreatePool("objectListTest"); err != nil {
		t.Error("Unable to create pool")
		return
	}

	pool, err := cluster.OpenPool("objectListTest")
	if err != nil {
		t.Error("Unable to open pool")
		return
	}

	pool.WriteFullToObject("object1", bytes.NewBufferString("data1"))
	pool.WriteFullToObject("object2", bytes.NewBufferString("data2"))
	pool.WriteFullToObject("object3", bytes.NewBufferString("data3"))

	objectList, err := pool.OpenObjectList()
	for i := 0; i < 3; i++ {
		objectId, locationKey, err := objectList.Next()
		t.Logf("%s:%s", objectId, locationKey)
		if err != nil {
			t.Error("error: ", err)
		}
	}
	_, _, err = objectList.Next()
	if err == nil {
		t.Error("should return error")
	}
	objectList.Close()

	if err := cluster.DeletePool("objectListTest"); err != nil {
		t.Error("Unable to delete pool")
		return
	}

}
