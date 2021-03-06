package grados

// import "testing"
// import "bytes"

// FIX this

// func TestPoolSnapshot(t *testing.T) {
// 	cluster := connect(t)
// 	if cluster == nil {
// 		return
// 	}
// 	pool, err := cluster.ManagePool("data")
// 	handleError(t, err)
// 	if pool == nil {
// 		return
// 	}

// 	bytes.NewBufferString("Hello World")

// 	pool.WriteFullToObject("test", bytes.NewBufferString("Hello World"))
// 	err = pool.CreatePoolSnapshot("my_snapshot")
// 	handleError(t, err)

// 	err = pool.WriteFullToObject("test", bytes.NewBufferString("Hello Mars"))
// 	handleError(t, err)

// 	err = pool.RollbackToPoolSnapshot("test", "my_snapshot")
// 	handleError(t, err)

// 	rolledback, err := pool.ReadFromObject("test", 11, 0)
// 	handleError(t, err)
// 	result := new(bytes.Buffer)
// 	result.ReadFrom(rolledback)
// 	t.Log("RolledBack:", result.String())

// 	snapshots := pool.ListPoolSnapshots()
// 	t.Log("snapshots:", len(snapshots))
// 	for _, s := range snapshots {
// 		t.Log("snapshot: ", s)
// 	}

// 	id, err := pool.LookupPoolSnapshot("my_snapshot")
// 	handleError(t, err)
// 	t.Log("Lookup result: ", id)

// 	name, err := pool.ReverseLookupSnapshot(id)
// 	handleError(t, err)
// 	t.Log("Reverse lookup result:", name)

// 	time, err := pool.SnapshotTimestamp(id)
// 	handleError(t, err)
// 	t.Log("time", time)

// 	err = pool.RemovePoolSnapshot("my_snapshot")
// 	handleError(t, err)

// 	snapshots = pool.ListPoolSnapshots()
// 	t.Log("snapshots:", len(snapshots))
// 	for _, s := range snapshots {
// 		t.Log("snapshot: ", s)
// 	}

// 	err = pool.RemoveObject("test")
// 	handleError(t, err)
// }

// func TestUseSnapshot(t *testing.T) {
// 	cluster := connect(t)
// 	if cluster == nil {
// 		return
// 	}
// 	defer cluster.Shutdown()

// 	err := cluster.CreatePool("my_pool")
// 	handleError(t, err)
// 	defer cluster.DeletePool("my_pool")

// 	pool, err := cluster.ManagePool("my_pool")
// 	handleError(t, err)
// 	defer pool.CloseNow()

// 	err = pool.WriteToObject("my_object", bytes.NewBufferString("data1"), 0)
// 	handleError(t, err)

// 	err = pool.CreatePoolSnapshot("snap1")
// 	handleError(t, err)

// 	err = pool.WriteToObject("my_object", bytes.NewBufferString("data2"), 0)
// 	handleError(t, err)

// 	id, err := pool.LookupPoolSnapshot("snap1")
// 	handleError(t, err)

// 	pool.UseSnapshot(id)

// 	data, err := pool.ReadFromObject("my_object", 5, 0)
// 	handleError(t, err)

// 	result := new(bytes.Buffer)
// 	result.ReadFrom(data)

// 	if "data1" != result.String() {
// 		t.Errorf("result should be data1, result is %s", result)
// 	}
// }

// func TestSelfManagedPool(t *testing.T) {
// 	cluster := connect(t)
// 	if cluster == nil {
// 		return
// 	}

// 	err := cluster.CreatePool("test")
// 	handleError(t, err)
// 	defer cluster.DeletePool("test")

// 	pool, err := cluster.ManagePool("test")
// 	handleError(t, err)
// 	if pool == nil {
// 		return
// 	}

// 	snapshot, err := pool.CreateSelfManagedSnapshot()
// 	handleError(t, err)
// 	if snapshot == nil {
// 		return
// 	}
// 	t.Log("created snapshot context:", snapshot.Id)

// 	snapshots := ManagedSnapshots{snapshot}
// 	err = snapshot.SetAsWriteContext(snapshots)
// 	handleError(t, err)
// 	t.Log("snapshot context set to read")

// 	err = pool.WriteFullToObject("sample", bytes.NewBufferString("This is a test"))
// 	handleError(t, err)
// 	t.Log("full object written")

// 	err = snapshot.Remove()
// 	handleError(t, err)

// }
