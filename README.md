# Chapel Mesos Framework

This project contains program source code that allows Chapel programs to run on a Mesos managed cluster/cloud computing environment.

## HOWTO make the Mesos Framework?

If you plan to use this framework with Chapel programs that do not use Chapel's HDFS I/O support, edit the `INSTALL_DIR` variable in `Makefile`, compile the Chapel-Mesos-Framework by running `make` and `make install`.

For programs that require HDFS I/O support, check to see if the machine compiling the Chapel program has `libjvm.a` on the system
(look in `/usr/lib`, `/usr/local/lib`, or where `$JAVA_HOME` points).

If `libjvm.a` exists, compilation of the Chapel-Mesos-Framework using HDFS I/O will be simple. Run `make` and `make install`.

In the event you have `libjvm.so` and access to `libjvm.a` is unavailable, open the Chapel-Mesos-Framework's Makefile and modify
the variable `LIBJAVADIR` such that it points to the directory of the `libjvm.so` file for each node on the cluster. If `libjvm.so`
is installed in different directories across multiple machines, list those locations in the follow fashion in the `Makefile`:

	LIBJAVADIR=<path-2-libjvm.so-A>:<path-2-libjvm.so-B>:<path-2-libjvm.so-C>

If `libjvm.so` is installed in the same location on all of the compute nodes, then modify `LIBJAVADIR` to the following format:

	LIBJAVADIR=<path-2-libjvm.so-A>

If the program does not require HDFS support or if you have completed modifying the Makefile, run `make` and `make install`. Provided
your build successfully completes, then continue reading the next section.

If your build did not complete successfully, check to make sure your `LD_LIBRARY_PATH` and `LD_LIBRARY_RUN` variables are pointed to
all the right locations (namely, where you have any required lib*.a or lib*.so files installed).

## HOWTO use the Mesos Framework?

For the purposes of this example, the compiled program is `$CHPL_HOME/examples/hello6-taskpar-dist.chpl`. It was compiled into `hello`
and `hello_real`. Copy `hello_real` to a directory on a distributed file system that each compute node can access (in this example, it
is presumed HDFS is being used for deploying compiled Chapel binaries and that $HDFS_URI is an HDFS directory that contains `hello_real`).

Remeber to compile Chapel programs for distributed compute, set the following environment variable prior to Chaple-program-compilation:

	$ export CHPL_COMM=gasnet

Chapel programs use the GASNet library to manage distributed compute operations. The following environment variables are used by GASNet
at runtime to do some initial orchestration needed to launch a Chapel program compiled for distributed computing. Here's a very quick 
summary of the variables and what the variables mean.

*	`$GASNET_SPAWNFN='C'` tells GASNet a customized launch program is being used to start up the program on distributed nodes
*	`$GASNET_MASTERIP` gives GASNet the hostname for the node that's launching the program
*	`$CSPAWN_ROUTE_OUTPUT` tells the distributed Chaple program to forward it's `STDOUT` to the launcher node's `STDOUT`.
*	`$GASNET_CSPAWN_CMD` is the customized command GASNet will use to launch the *_real part of the Chapel program on the cluster.

In order to run a Chapel program on the Mesos Cluster using the scheduler in this project, set the following environment variables:

	$ export GASNET_SPAWNFN='C'
	$ export GASNET_MASTERIP='$(hostname)'
	$ export CSPAWN_ROUTE_OUTPUT=1
	$ export GASNET_CSPAWN_CMD="nohup chpl-scheduler --master $ZOOKEEPER_MASTER_IP --exec-uri $HDFS_URI/hello_real --cpus %N --cores 0.25 --mem 256MB --remote-cmd '%C'&"

Make sure hello_real is copied to $HDFS_URI! Then run your Chapel program:

	$./hello -nl 2

This should start the mesos scheduler for Chapel (the `chpl-scheduler` found in `$GASNET_CSPAWN_CMD`). `chpl-scheduler` will acquire
resources, download a copy of `hello_real` to the acquired remote nodes, and start `hello` in distributed compute mode.

Each node will use 0.25% of the available cores (for thread processing) and have 256MB of RAM allocated for use.

If the framework is: still active and all the tasks have completed (Mesos says the jobs are finished, lost, or failed), OR if the 
framework is active and can't get resources, OR if some of the resources failed during execution (you got bored and hit `CTL-C` and
reviewed the `nohup.out`), run this command:

	$ echo "frameworkId=$FRAMEWORK_ID" | curl -d@- -X POST http://$ZOOKEEPER_MASTER_IP/master/shutdown

*	`$FRAMEWORK_ID` is an identifier Mesos assigns to each active (instanced) framework; the variable needs to be set before 
running this command.

Another option is to run `ps` in the terminal you ran `./hello -nl 2` in and then kill the `chpl-scheduler` process that is highly
likely to be running (it continues to run even after a Chapel program successfully runs, terminates, and returns control of the
command line to the user - in the case the Chapel fails, control of the command line does not return to the user - both are bugs
that need further work/attention).

Information about interactions between `chpl-scheduler` and Mesos during the task assignment/execution process can be found in the
`nohup.out` file.

Point-of-contact: ct.clmsn at gmail dot com 

