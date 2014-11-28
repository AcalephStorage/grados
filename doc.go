/*
Package grados is a go library for communicating with librados. This requires that the host machine this runs on has ceph
and the librados library.

Usage

To start communicating with ceph, a connection to a cluster needs to be established.

Connect to the default cluster using the configuration found in /etc/ceph:

		// connect to default cluster
		cluster, err := rados.ConnectToDefaultCluster()

		// disconnect from cluster
		cluster.Shutdown()


Basic pool operations can be done:

		// create a pool
		err := cluster.CreatePool("new_pool")

		// deletes a pool
		err := cluster.DeletePool("new_pool")

		// manage a pool
		pool, err := cluster.CreatePool("new_pool")

		// stop managing the pool
		pool.Close()

Basic object operations:

		// manage an object
		object := pool.ManageObject("object_name")

		// write to object
		err := object.WriteFull(my_data_reader)

		// read from an object
		reader, err := object.Read(lengthToRead, offset)

		// remove an object
		err := object.Remove()

Async object operations:

		// manage object asynchronously
		asyncObject := pool.ManageObject("my_object").AsyncMode(completeCallback, safeCallback, errCallback, "arg1", "arg2")

		// async write
		asyncObject.WriteFull(my_data_reader)

		// async read. result will be stored in args.
		asyncObject.Read(10, 0)

		// async remove.
		asyncObjet.Remove()

Other features implemented are:
 - pool snapshots
 - managed-snapshots
 - read/write transactions
 - object extended attributes

Missing implementation:
 - OMAP/TMAP operations (TODO)
 - class executions (TODO)
 - mon/osd/pg commands (necessary?)
 - watch/unwatch/notify objects (still looking for a way to do this)
*/
package grados
