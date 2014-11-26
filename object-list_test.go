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

	pool.ManageObject("object1").WriteFull(bytes.NewBufferString("data1"))
	pool.ManageObject("object2").WriteFull(bytes.NewBufferString("data2"))
	pool.ManageObject("object3").WriteFull(bytes.NewBufferString("data3"))

	objectList, err := pool.OpenObjectList()
	for i := 0; i < 3; i++ {
		object, locationKey, err := objectList.Next()
		t.Logf("%s:%s", object.Name, locationKey)
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
