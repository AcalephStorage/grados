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

	object := pool.ManageObject("object1")

	if err := object.WriteFull(bytes.NewBufferString("data1")); err != nil {
		t.Error("Unable to write object")
		return
	}

	object.SetAttribute("attrib1", bytes.NewBufferString("attrib1"))
	object.SetAttribute("attrib2", bytes.NewBufferString("attrib2"))
	object.SetAttribute("attrib3", bytes.NewBufferString("attrib3"))

	attribList, err := object.OpenAttributeList()
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
	attribList.Close()

	if err := cluster.DeletePool("objectListTest"); err != nil {
		t.Error("Unable to delete pool")
		return
	}

}
