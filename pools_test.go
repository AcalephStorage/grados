package grados

import "testing"

func TestGetConfigReference(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	pool, err := cluster.ManagePool("data")
	handleError(t, err)
	config := pool.Config()
	t.Log("config context:", config.context)
}

func TestGetClusterReference(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	pool, err := cluster.ManagePool("data")
	handleError(t, err)
	c := pool.Cluster()
	fsid := c.FSID()
	t.Log("FSID:", fsid)
	if cluster.handle != c.handle {
		t.Errorf("handles not the same: %v, %v", cluster.handle, c.handle)
	}
}

func TestRequiresAlignment(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	pool, err := cluster.ManagePool("data")
	handleError(t, err)
	alignment := pool.RequiresAlignment()
	t.Log("RequiresAlignment:", alignment)
}

func TestRequiredAlignemnt(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	pool, err := cluster.ManagePool("data")
	handleError(t, err)
	alignment := pool.RequiredAlignment()
	t.Log("RequiredAlignment:", alignment)
}

func TestListPools(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	pools, err := cluster.ListPools()
	handleError(t, err)
	if pools != nil {
		t.Log("Pools:", pools)
	}
	if len(pools) <= 0 {
		t.Error("No pools found.")
	}
	cluster.Shutdown()
}

func TestPoolLookup(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	id, err := cluster.LookupPool("data")
	handleError(t, err)
	t.Log("Pool ID:", id)
	if id != 0 {
		t.Errorf("id should be 0, id is %d", id)
	}
	cluster.Shutdown()
}

func TestPoolReverseLookup(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	poolName, err := cluster.ReverseLookupPool(0)
	handleError(t, err)
	t.Log("Pool:", poolName)
	if "data" != poolName {
		t.Errorf("Name should be data, name is %s", poolName)
	}
	cluster.Shutdown()
}

func TestCreatePool(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	x := cluster.CreatePool("doood")
	handleError(t, x)
	y := cluster.DeletePool("doood")
	handleError(t, y)
	cluster.Shutdown()
}

func TestCreatePoolWithAuid(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	err := cluster.CreatePoolWithOwner("doood", 1)
	handleError(t, err)
	err = cluster.DeletePool("doood")
	handleError(t, err)
	cluster.Shutdown()
}

func TestCreatePoolWithCrushRule(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	err := cluster.CreatePoolWithCrushRule("doood", 0)
	handleError(t, err)
	err = cluster.DeletePool("doood")
	handleError(t, err)
	cluster.Shutdown()
}

func TestCreatePoolWithAll(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	err := cluster.CreatePoolWithAll("doood", 0, 0)
	handleError(t, err)
	err = cluster.DeletePool("doood")
	handleError(t, err)
	cluster.Shutdown()
}

func TestGetPoolStats(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	pool, err := cluster.ManagePool("data")
	handleError(t, err)
	stats, err := pool.Status()
	handleError(t, err)
	t.Logf("STATS: %v", stats)
	pool.Close()
	cluster.Shutdown()
}

func TestAuid(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	pool, err := cluster.ManagePool("data")
	handleError(t, err)
	auid, err := pool.AUID()
	handleError(t, err)
	t.Log("AUID:", auid)
	err = pool.SetAUID(1)
	handleError(t, err)
	err = pool.SetAUID(auid)
	handleError(t, err)
	pool.Close()
	cluster.Shutdown()
}

func TestId(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	pool, err := cluster.ManagePool("data")
	handleError(t, err)
	id := pool.Id()
	if id != 0 {
		t.Errorf("Id should be 0, id is %d", id)
	}
	pool.Close()
	cluster.Shutdown()
}

func TestName(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	pool, err := cluster.ManagePool("data")
	handleError(t, err)
	name := pool.Name()
	handleError(t, err)
	if name != "data" {
		t.Errorf("Name should be data, id is %s", name)
	}
	pool.Close()
	cluster.Shutdown()
}
