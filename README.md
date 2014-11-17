grados
======

grados is a go library for communicating with librados. This requires that the host machine this runs on has ceph and the librados library.

Usage

To start communicating with ceph, a connection to a cluster needs to be established. There are several ways of connecting to a ceph cluster.

Connect to the default cluster using the configuration found in /etc/ceph:

```
cluster, err := rados.ConnectToDefaultCluster()
```

Or Connect using an existing config context:

```
config := cluster1.Config()
cluster2, err := rados.ConnectWithExistingConfig(config)
```

We can use a different configuration file:

```
cluster, err := rados.new(Connection).UseConfigFile("/path/to/config/file").Connect()
```

Or use command-line args as configuration:

```
cluster, err := rados.new(Connection).UseConfigArgs().Connect()
```

Or an environment variable as configuration:

```
cluster, err := rados.new(Connection).UseConfigEnv("NAME_OF_ENV_VAR").Connect()
```

Or a map[string]string containing the connection configuration:

```
cluster, err := rados.new(Connection).UseConfigMap(configMap).Connect()
```

We can also connect to a cluster with a given user (omit the "client." prefix):

```
cluster, err := rados.new(Connection).As("admin").Connect()
```

Or connect with a given cluster name and user (fully qualified user name):

```
cluster, err := rados.new(Connection).To("ceph").As("client.admin").Connect()
```

We can mix and match these configuration as we need:

```
cluster, err := rados.new(Connection).To("ceph").As("client.admin").UseConfigFile("/etc/ceph/myceph.conf").Connect()
```

Once a connection has been made, we can now query or execute commands to the cluster.

```
// get the cluster fsid
fsid := cluster.FSID()
```

## More info [here](http://godoc.org/github.com/AcalephStorage/grados)

_Still WIP_