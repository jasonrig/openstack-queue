openstack-queue
===============

a batch job queueing system for OpenStack-based cloud services

building and installing
-----------------------

Here are some steps for building and installing on Ubuntu Trusty. Easily transferrable to other distros!

1. Get the dependencies: `apt-get install maven2 openjdk-7-jdk redis-server`
2. Clone this repository: `git clone https://github.com/jasonrig/openstack-queue.git`
3. Build the source: `mvn install`
4. Move the jars somewhere you think is suitable, e.g. /opt/openstack-queue/:
    ```
    mkdir /opt/openstack-queue
    cp ./target/openstack-queue-0.0.1-SNAPSHOT.jar /opt/openstack-queue/
    cp -R ./target/dependency-jars/ /opt/openstack-queue/
    ```
5. Create a directory for configuration files: `mkdir /etc/openstack-queue`
6. Create a user account under which the queue will run: `sudo useradd -d /etc/openstack-queue -r -s /bin/false openstack-queue`
7. Create a configuration file (warning: should be readable __only__ by root and the openstack-queue user or else other users may take control of your VMs). Use the openstack-queue.properties example file as a template. It should be stored in the home directory of the openstack-queue user, i.e. /etc/openstack-queue, and must be called openstack-queue.properties. 

    If you run the queue interactively, i.e. not as a service, the configuration file location should be as described by the Apache Commons Configuration docs (http://commons.apache.org/proper/commons-configuration/userguide/howto_properties.html):
    * in the current directory
    * in the user home directory
    * in the classpath
6. Create an upstart script so you can run this as a service. Here is an example (/etc/init/openstack-queue.conf):
    ```
    description     "OpenStack Queue"
    stop on runlevel [!2345]

    umask 002

    script
        mkdir -p /tmp/openstack-queue-tmp/
        chown openstack-queue:root /tmp/openstack-queue-tmp/
        chmod 700 /tmp/openstack-queue-tmp
        cd /tmp/openstack-queue-tmp/
        su -p -s /bin/bash -c "/usr/bin/java -jar /opt/openstack-queue/openstack-queue-0.0.1-SNAPSHOT.jar" openstack-queue >> /var/log/openstack-queue.log 2>&1
    end script
    ```
    This will create a space in /tmp where openstack-queue will store its temporary files, and log to /var/log/openstack-queue.log. This being a development version, lots of logs are produced and this file will get big quickly. To reduce the verbosity, edit src/main/resources/logback.xml and rebuild the jar.
7. Start the required services:
  * `service redis-server start`
  * `service openstack-queue start`
 
Notes on security: This queue trusts the users to do the right thing! Any user may delete or submit or even terminate the queue by sending the correct commands to the redis message queue. The best way to implement this as far as I see it is to use redis authentication and then create a client that will enforce whatever security policies you want. This has not been done yet!


emulating PBS(ish) qsub, qstat and qterm
----------------------------------------
Here are some crude examples of how to emulate qsub, qstat and qterm in my own special way. Yes, I use PHP... sorry. But it works!

1. Get the dependencies: `apt-get install php5-cli php5-redis php5-json`
2. Create scripts as desired:


qstat
```
#!/usr/bin/php
<?php

function processRedisMessage($redis, $channel, $message) {
        $redis->close();
        $queueStatus = json_decode($message);

        echo "Job ID\tName\tStart time\tLatest finish\tStatus\n";
        foreach ($queueStatus as &$job) {
                $job = get_object_vars($job);
                $startTime = ($job['startTime']==0)?"not started":date("r", floor($job['startTime'] / 1000));
                $latestFinish = ($job['timeLimit'] == 0)?"unlimited":date("r", floor(($job['startTime']+$job['timeLimit']) / 1000));
                echo $job['id']."\t".$job['name']."\t".$startTime."\t".$latestFinish."\t".$job['status']."\n";
        }

}

$redis=new Redis() or die("PHP redis package not available.");
$redis->connect('127.0.0.1');

echo "Waiting for queue status...\n";
$redis->subscribe(array('jobqueue-status'), 'processRedisMessage');

?>
```


qsub
```
#!/usr/bin/php
<?php

$redis=new Redis() or die("PHP redis package not available.");
$redis->connect('127.0.0.1');

$jobData = file_get_contents($argv[1]);
$redis->publish('jobqueue-submit', $jobData);

?>
```

qdel
```
#!/usr/bin/php
<?php

$redis=new Redis() or die("PHP redis package not available.");
$redis->connect('127.0.0.1');

$redis->publish('jobqueue-admin', "killjob ".$argv[1]);

?>
```

qterm
```
#!/usr/bin/php
<?php

$redis=new Redis() or die("PHP redis package not available.");
$redis->connect('127.0.0.1');

$redis->publish('jobqueue-admin', "shutdown");

?>
```

a "hello world" MPI job
-----------------------
1. Get the build dependencies: `apt-get install build-essential libopenmpi-dev openmpi-bin`
2. Build the following MPI code (taken from http://mpitutorial.com/mpi-hello-world/) using mpicc:
    ```
   #include <mpi.h>
 
   int main(int argc, char** argv) {
     // Initialize the MPI environment
     MPI_Init(NULL, NULL);
   
     // Get the number of processes
     int world_size;
     MPI_Comm_size(MPI_COMM_WORLD, &world_size);
 
     // Get the rank of the process
     int world_rank;
     MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
 
     // Get the name of the processor
     char processor_name[MPI_MAX_PROCESSOR_NAME];
     int name_len;
     MPI_Get_processor_name(processor_name, &name_len);
 
     // Print off a hello world message
     printf("Hello world from processor %s, rank %d"
            " out of %d processors\n",
            processor_name, world_rank, world_size);
 
     // Finalize the MPI environment.
     MPI_Finalize();
   }
    ```
3. Create a submit json file (note: logPath and resultsPath must be writable by the openstack-queue user or group):
    ```
{
        'groupName':'hello',
        'minNodes':1,
        'maxNodes':1,
        'minNodeSize':8,
        'logPath':'/somewhere/to/store/server/logs/',
        'resultsPath':'/somewhere/to/copy/results/files/',
        'payloadFiles':['/path/to/compiled/program/a.out'],
        'bootstrapScript':'apt-get -y update && apt-get -y install openmpi-bin',
        'executeScript':'mpirun --hostfile /tmp/hostfile /tmp/a.out',
        'cleanupScript':''
}

    ```
4. Submit the json file, e.g. qsub myjob.json
5. Monitor the logs to see it in action `tail -f /var/log/openstack-queue.log` or check with qstat

some notes on the json file
---------------------------

The example given above is about the most minimal set of parameters required for something to run. The fields are as follows:

1. groupName: The name given to the server group used for this calculation. Synonymous with a job name.
2. minNodes: The smallest acceptable number of nodes before this job can run
3. maxNodes: The maximum number of nodes that can be allocated. The queue aims for this number.
4. minNodeSize: The smallest number of CPUs in the nodes created
5. logPath: Where stdout and stderr for scripts executed on the nodes are stored (two files per node)
6. resultsPath: Any data stored in /tmp/copyback on the nodes created will be returned to this directory in gzipped tarballs
7. payloadFiles: Data required for the computation that is copied to every node created
8. bootstrapScript: A script run after the payload is copied but before the job begins. Suitable for installing dependencies.
9. executeScript: A script run after boostrapping is complete in order to run the real calculation
10. cleanupScript: Run after the execute script is complete. Suitable for putting relevant results files into /tmp/copyback

There are more possible fields that are not listed here. See the javadoc for `jrigby.openstack_queue.request_templates.ThreeStageJobResourceRequestJsonMessage` for a complete list.

more notes
----------
* All scripts run as root on the compute nodes created. This isn't a big issue since they generally don't have privileged access to anything outside of the confines of the calculation/job you're running.
* Payload files are given 777 permissions
* The IP address of each participating node is printed at the top of each server log file
* The private key for the default user of the VM image is also written to the logPath. __This should be kept somewhere secret where only you and the openstack-queue user may read/write, otherwise someone could take over your VMs.__ Not a huge issue in the end though, since these servers only last for the duration of the job.
