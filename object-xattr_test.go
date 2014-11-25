package grados

import "testing"
import "bytes"

func TestIterateAttrList(t *testing.T) {
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

	if err := pool.WriteFullToObject("object1", bytes.NewBufferString("data1")); err != nil {
		t.Error("Unable to write object")
		return
	}

	pool.SetObjectAttribute("object1", "attrib1", bytes.NewBufferString("attrib1"))
	pool.SetObjectAttribute("object1", "attrib2", bytes.NewBufferString("attrib2"))
	pool.SetObjectAttribute("object1", "attrib3", bytes.NewBufferString("attrib3"))

	attribList, err := pool.OpenAttributeList("object1")
	for i := 0; i < 3; i++ {
		name, value, err := attribList.Next()
		buf := new(bytes.Buffer)
		buf.ReadFrom(value)
		t.Logf("%s:%s", name, buf.String())
		if err != nil {
			t.Error("error: ", err)
		}
	}
	_, _, err = attribList.Next()
	if err == nil {
		t.Error("should return error")
	}
	attribList.End()

	if err := cluster.DeletePool("objectListTest"); err != nil {
		t.Error("Unable to delete pool")
		return
	}

}
