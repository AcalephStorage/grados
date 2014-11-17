package grados

import "testing"

func TestGetClusterStats(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	stats, err := cluster.Status()
	handleError(t, err)
	t.Logf("STATS: %v", stats)
	cluster.Shutdown()
}

func TestGetFSID(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	fsid := cluster.FSID()
	t.Log("FSID: ", fsid)
	cluster.Shutdown()
}

func TestInstanceId(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	id := cluster.InstanceId()
	t.Log("InstanceID: ", id)
	cluster.Shutdown()
}

func TestGetConfig(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	val, err := cluster.GetConfigValue("auth cluster required")
	handleError(t, err)
	t.Log("CONFIG: ", val)
	if "cephx" != val {
		t.Errorf("value should be cephx, value is %s", val)
	}
	cluster.Shutdown()
}

func TestWaitForLatestOsdMap(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	err := cluster.WaitForLatestOsdMap()
	handleError(t, err)
	cluster.Shutdown()
}
