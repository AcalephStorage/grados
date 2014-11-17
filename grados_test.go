package grados

import "testing"

func TestVersion(t *testing.T) {
	version := Version()
	t.Logf("Version: %s", version)
}

func TestConnect(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	cluster.Shutdown()
}

func TestFunkyConnect(t *testing.T) {
	cluster, err := new(Connection).To("ceph").As("client.admin").UseConfigFile("/etc/ceph/ceph.conf").Connect()
	handleError(t, err)
	if cluster != nil {
		cluster.Shutdown()
	}
	cluster, err = new(Connection).As("admin").Connect()
	handleError(t, err)
	if cluster != nil {
		cluster.Shutdown()
	}
}

func TestConnectToExistingConfig(t *testing.T) {
	c1 := connect(t)
	if c1 == nil {
		return
	}
	config := c1.Config()
	c2, err := ConnectWithExistingConfig(config)
	handleError(t, err)
	if c2 == nil {
		t.Error("c2 should not be nil")
	}
	if c1.FSID() != c2.FSID() {
		t.Error("did not connect to the same cluster")
	}
}

func TestPingMonitor(t *testing.T) {
	cluster := connect(t)
	if cluster == nil {
		return
	}
	result, err := cluster.PingMonitor("acaleph")
	handleError(t, err)
	t.Logf("PING: %v", result)
	cluster.Shutdown()
}

func connect(t *testing.T) *Cluster {
	cluster, err := ConnectToDefaultCluster()
	handleError(t, err)
	return cluster
}

func handleError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("ERROR: %s\n", err.Error())
	}
}
