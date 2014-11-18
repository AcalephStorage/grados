package grados

/*
#cgo LDFLAGS: -lrados

#include <stdlib.h>
#include <rados/librados.h>
*/
import "C"

import (
	"fmt"
	"os"
	"unsafe"
)

const (
	errorRange             = -34
	errorNameTooLong C.int = -36
)

// RadosError contains the error code returned from call the librados functions. The message is some (maybe) helpful
// text regarding the error.
type RadosError struct {
	Code    int
	Message string
}

// toRadosError converts a C.int return from librados to a RadosError.
func toRadosError(err C.int) *RadosError {
	if err < 0 {
		return &RadosError{
			Code: int(err),
		}
	}
	return nil
}

// Implement the error interface.
func (err *RadosError) Error() string {
	return fmt.Sprintf("%d: %s", err.Code, err.Message)
}

// ClusterConfig represents a config handle of a ceph cluster.
type ClusterConfig struct {
	context C.rados_config_t
}

// Cluster represents a Cluster handle encapsulating the functions in librados that requires a cluster handle.
type Cluster struct {
	handle C.rados_t
}

// Connection represents a connection to a ceph cluster. Calling the Connect() function will connect to a ceph cluster
// using whatever has been set in this struct. There are also helper functions to configure a connection. See To(), As()
// UnseConfigFile(), UseConfigArgs(), UseConfigEnv(), and UseConfigMap().
type Connection struct {

	// The cluster name of ceph cluster to connect to. If this is left as an empty string adn the UserName is also
	// empty, the default configuration will be used. If this is specified, the UserName should also be specified and
	// should be fully qualified (eg. client.admin instead of just admin)
	ClusterName string

	// The UserName to connect as. This should be the fully qualified user name if the cluster name is specified. If the
	// cluster name is not specified, this should just be the user name without the "client." prefix. This can be empty
	// if the ClusterName is empty and the defaults will be used.
	UserName string

	// The ceph config file path. This should be a path to the ceph configuration file. If this is empty and all other
	// configuration settings are also empty, then the default configuration will be used.
	ConfigFile string

	// Parse the command line arguments for configuration. This will use the command line arguments as the configuration
	// to connect to the ceph cluster. If this is false and all other configuration settings are also emtpy, the default
	// configuration will be used.
	ParseArgs bool

	// Use an environment variable to specify the configuration. The environment variable should have the command line
	// flags for configuration. If this is empty and all other configuration settings are empty, the default
	// configuration will be used.
	ConfigEnv string

	// Use a map[string]string to use as the K/V configuration. If this is empty and all other configuration settings
	// are empty, the default configuration will be used.
	ConfigMap map[string]string

	// This represents the cluster handle for communicating with the ceph cluster. This will be instantiated when the
	// Connect() function is successfully called.
	cluster *Cluster
}

// ConnectToDefaultCluster will connect to the default cluster based on configuration in /etc/ceph/.
func ConnectToDefaultCluster() (*Cluster, error) {
	return new(Connection).Connect()
}

func ConnectWithExistingConfig(config *ClusterConfig) (*Cluster, error) {
	conn := &Connection{
		cluster: new(Cluster),
	}
	ret := C.rados_create_with_context(&conn.cluster.handle, config.context)
	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to create cluster handle with existing config context"
		return nil, err
	}

	ret = C.rados_connect(conn.cluster.handle)
	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to connect to cluster. Make sure cluster is accessible and configuration is correct."
		return nil, err
	}

	return conn.cluster, nil
}

// To will set the cluster name to connect to.
func (conn *Connection) To(cluster string) *Connection {
	conn.ClusterName = cluster
	return conn
}

// As will set the cluster user name to connect as.
func (conn *Connection) As(user string) *Connection {
	conn.UserName = user
	return conn
}

// UseConfigFile will user a custom configuration file instead of the default.
func (conn *Connection) UseConfigFile(configFile string) *Connection {
	conn.ConfigFile = configFile
	return conn
}

// UseConfigArgs will load configuration from the command line args.
func (conn *Connection) UseConfigArgs() *Connection {
	conn.ParseArgs = true
	return conn
}

// UseConfigEnv will load configuration from the given environment variable.
func (conn *Connection) UseConfigEnv(env string) *Connection {
	conn.ConfigEnv = env
	return conn
}

// UserConfigMap will load configuration based on the content of the configMap.
func (conn *Connection) UseConfigMap(configMap map[string]string) *Connection {
	conn.ConfigMap = configMap
	return conn
}

// Connect will connect to the cluster based on the values it contains.
func (conn *Connection) Connect() (*Cluster, error) {
	if err := conn.createClusterHandle(); err != nil {
		return nil, err
	}

	if err := conn.configure(); err != nil {
		return nil, err
	}

	ret := C.rados_connect(conn.cluster.handle)
	if err := toRadosError(ret); err != nil {
		err.Message = fmt.Sprintf("Unable to connect to cluster. Make sure cluster is accessible and configuration is correct. %v", conn)
		return nil, err
	}
	return conn.cluster, nil
}

// Shutdown will close the connection to the cluster.
func (cluster *Cluster) Shutdown() {
	C.rados_shutdown(cluster.handle)
}

// create the cluster handle with the ClusterName and UserName configured.
func (conn *Connection) createClusterHandle() error {
	conn.cluster = new(Cluster)
	var ret C.int

	switch {

	// use rados_create2() since cluster and user are both specified
	case conn.ClusterName != "" && conn.UserName != "":
		cluster := C.CString(conn.ClusterName)
		user := C.CString(conn.UserName)
		defer C.free(unsafe.Pointer(cluster))
		defer C.free(unsafe.Pointer(user))
		ret = C.rados_create2(&conn.cluster.handle, cluster, user, 0)

	// use rados_create() with the given UserName
	case conn.ClusterName == "" && conn.UserName != "":
		user := C.CString(conn.UserName)
		defer C.free(unsafe.Pointer(user))
		ret = C.rados_create(&conn.cluster.handle, user)

	// use rados_create() with a nil user
	case conn.ClusterName == "" && conn.UserName == "":
		ret = C.rados_create(&conn.cluster.handle, nil)

	default:
		return &RadosError{
			Code:    -2,
			Message: "Unable to create cluster handle. If cluster name is specified, also include qualified user name.",
		}
	}

	if err := toRadosError(ret); err != nil {
		err.Message = "Unable to create cluster handle. Make sure you have access to the ceph cluster."
		return err
	}
	return nil
}

func (conn *Connection) configure() error {

	// use default configuration
	if conn.ConfigFile == "" && !conn.ParseArgs && conn.ConfigEnv == "" && len(conn.ConfigMap) == 0 {
		ret := C.rados_conf_read_file(conn.cluster.handle, nil)
		if err := toRadosError(ret); err != nil {
			err.Message = "Unable to load default configuration."
			return err
		}
		return nil
	}

	// use config file if specified
	if conn.ConfigFile != "" {
		config := C.CString(conn.ConfigFile)
		defer C.free(unsafe.Pointer(config))
		ret := C.rados_conf_read_file(conn.cluster.handle, config)
		if err := toRadosError(ret); err != nil {
			err.Message = "Unable to load configuration file. Make sure it exists and is accessible."
			return err
		}
	}

	// use config env if specified
	if conn.ConfigEnv != "" {
		env := C.CString(conn.ConfigEnv)
		defer C.free(unsafe.Pointer(env))
		ret := C.rados_conf_parse_env(conn.cluster.handle, env)
		if err := toRadosError(ret); err != nil {
			err.Message = "Unable to load configuration from env."
			return err
		}
	}

	// use config from args
	if conn.ParseArgs {
		argc := C.int(len(os.Args))
		argv := make([]*C.char, argc)
		for i, arg := range os.Args {
			argv[i] = C.CString(arg)
			defer C.free(unsafe.Pointer(argv[i+1]))
		}
		ret := C.rados_conf_parse_argv(conn.cluster.handle, argc, &argv[0])
		if err := toRadosError(ret); err != nil {
			err.Message = "Unable to load configuration from args."
			return err
		}
	}

	if conn.ConfigMap != nil || len(conn.ConfigMap) > 0 {
		for k, v := range conn.ConfigMap {
			key := C.CString(k)
			val := C.CString(v)
			defer C.free(unsafe.Pointer(key))
			defer C.free(unsafe.Pointer(val))
			ret := C.rados_conf_set(conn.cluster.handle, key, val)
			if err := toRadosError(ret); err != nil {
				err.Message = fmt.Sprintf("Unable to load config %s=%s", key, val)
				return err
			}
		}
	}

	return nil
}

// Version will return current librados version.
func Version() string {
	var major, minor, extra C.int
	C.rados_version(&major, &minor, &extra)
	return fmt.Sprintf("v%d.%d.%d", major, minor, extra)
}
