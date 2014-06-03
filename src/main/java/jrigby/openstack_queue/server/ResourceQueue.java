package jrigby.openstack_queue.server;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.jclouds.http.HttpResponseException;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;
import org.jclouds.openstack.nova.v2_0.domain.KeyPair;
import org.jclouds.openstack.nova.v2_0.domain.Server;
import org.jclouds.openstack.nova.v2_0.domain.Server.Status;
import org.jclouds.openstack.nova.v2_0.options.CreateServerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

/**
 * The job queue. Coordinates job execution and ensures the cloud resources
 * are used within the predefined limits.
 * 
 * @author jason
 *
 */
public class ResourceQueue extends LinkedBlockingDeque<ResourceRequest> implements Runnable, Closeable {

	private final static Logger logger = LoggerFactory.getLogger(ResourceQueue.class);
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4483888804789587763L;
	
	/**
	 * Thread pool that limits the number of servers being concurrently
	 * created. This setting is defined in a configuration file.
	 * @see ServerSettings
	 */
	final private ExecutorService serverSchedulingThreadPool = Executors.newFixedThreadPool(ServerSettings.getInstance().getLimitsMaxSimultaneousServerScheduling());
	
	final private ExecutorService runningJobThreadPool = Executors.newCachedThreadPool();
	final private ExecutorService jobKillerThreadPool = Executors.newSingleThreadExecutor();
	
	private boolean isRunning; // A flag used to gracefully exit the queue polling loop
	private OSConnection connection;
	
	/**
	 * Limits the amount of time spend waiting for a server to start.
	 * Servers that take longer than this are presumed to have failed.
	 * This setting is defined in a configuration file.
	 * @see ServerSettings
	 */
	private Long serverSchedulingTimeLimit = ServerSettings.getInstance().getLimitsServerSchedulingTimeout();
	
	/**
	 * Limits the number of retries if a server creation attempt fails with
	 * an unspecified error from Nova. This is not the same as a server
	 * failing because there are not enough cloud resources available
	 * This setting is defined in a configuration file.
	 * @see ServerSettings
	 */
	private int maxServerCreationAttempts = ServerSettings.getInstance().getLimitsMaxServerSchedulingAttempts();
	
	/**
	 * Sets the maximum number of VM instances allowed.
	 * This setting is defined in a configuration file.
	 * @see ServerSettings
	 */
	private int instanceQuota = ServerSettings.getInstance().getLimitsMaxInstances();
	
	/**
	 * Sets the maximum number of cores allowed.
	 * This setting is defined in a configuration file.
	 * @see ServerSettings
	 */
	private int coreQuota = ServerSettings.getInstance().getLimitsMaxCores();
	
	/**
	 * Maintains a list of jobs that are currently running
	 */
	private Map<JobStatus, ServerCollection> runningJobs;
	
	/**
	 * Maintains a list of the {@link java.util.concurrent.Future} objects to terminate
	 * the threads running the jobs
	 */
	private Map<Integer, Future<?>> runningJobFutures;
	
	/**
	 * Maintains a set of jobs that are waiting to begin in addition to the jobs
	 * waiting in the queue. These jobs are either being scheduled or provisioned.
	 */
	private Set<JobStatus> startingJobs;
	
	/**
	 * A status designation. At any given time, unless the job has finished,
	 * it will be either QUEUED, SCHEDULING, PROVISIONING or RUNNING.
	 * 
	 * @author jason
	 *
	 */
	private enum QueueStatus {
		
		QUEUED("WAITING"),
		SCHEDULING("SCHEDULING"),
		PROVISIONING("PROVISIONING"),
		RUNNING("RUNNING");
		
		private String status;
		private QueueStatus(String status) {
			this.status = status;
		}
		
		/**
		 * Gets a string representation of the designated status
		 * @return string representation of designated status
		 */
		public String toString() {
			return status;
		}
	}
	
	/**
	 * Initialises the queue, linking it to a particular {@link OSConnection}.
	 */
	public ResourceQueue(OSConnection connection) {
		super();
		this.connection = connection;
		runningJobs = new ConcurrentHashMap<JobStatus, ServerCollection>();
		runningJobFutures = new ConcurrentHashMap<Integer, Future<?>>();
		startingJobs = Collections.synchronizedSet(new TreeSet<JobStatus>());
	}

	/**
	 * Starts the queue thread
	 * @return the thread running the queue
	 */
	public Thread startQueue() {
		Thread t = new Thread(this);
		t.start();
		return t;
	}

	/**
	 * Queue polling loop. Runs until the queue is closed.
	 */
	public void run() {
		isRunning = true;
		while (isRunning) {
			final ResourceRequest req;
			try {
				Thread.sleep(10000); // Avoid starting jobs rapidly; check the queue ever 10 seconds
				req = this.poll(1, TimeUnit.MINUTES); // Wait for a job to arrive for a maximum of 1 minute
			} catch (InterruptedException e) {
				close();
				break;
			}
			
			// Check if any jobs have run beyond their time limit
			Runnable jobKiller = new Runnable() {
				public void run() {
					killJobsOverTimeLimit();
				}
			};
			Future<?> jobKillerThreadFuture = jobKillerThreadPool.submit(jobKiller);
			
			// Attempt to start the job received...
			try {
				if (req != null) {
					logger.debug("Processing resource request for job \""+req.getGroupName()+"\"...");
					logger.debug("This job has been allocated the ID of "+req.getId());
					logger.debug("Note: Job IDs are not persistent and only unique for the duration of the queue.");
				
					// Check if request will exceed quota
					logger.debug("Checking if this request is able to run given queue limits...");;
					if (acceptableRequest(req)) {
						logger.debug("This job should run. Resources will be allocated shortly.");
						allocate(req);
					} else {
						// Job will exceed the quotas. Check if it's a permanent problem with this request
						if ((req.getMinNodes() > getInstanceQuota()) || 
								(req.getMinNodes() * req.getMinNodeSize() > getCoreQuota())) {
							// permanent failure
							logger.debug("This job will not run because it exceeds queue limits. Giving up.");
							req.doFailure(new ResourcesPermanentlyExceededException());
						} else {
							// temporary failure... if doFailure returns true, reschedule
							logger.debug("The queue is temporarily unable to accommodate this job.");
							if (req.doFailure(new ResourcesTemporarilyExceededException())) {
								logger.debug("Rescheduling the job.");
								try {
									ResourceQueue.this.putFirst(req);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							} else {
								logger.debug("Giving up.");
							}
							setQueueStatusCompleted(req); // Remove request from job lists
						}
					}
					
				}
				
			// This catch clause is to catch communication errors with nova that cause the thread to exit.
			// HttpResponseException is an unchecked exception, and can originate from any number of methods
			// that interact with nova.
			} catch (HttpResponseException e) {
				logger.error("An error was encountered processing resource request.");
				logger.error(e.getMessage());
				logger.error("This error was most likely encountered during communication with nova.");
				logger.error("As this is likely to be temporary (e.g. network timeouts, connectivity issues), this job will be resheduled.");
				try {
					putFirst(req); // Reschedule job
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
			
			try {
				jobKillerThreadFuture.get(); // Wait for the job killer thread to finish before continuing
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Checks if a job will fit in the project allocation and, if necessary,
	 * adjusts the job size to fit. This will be within the ranges allowed
	 * in the {@link ResourceRequest}. If the method returns true, it is
	 * possible that the maximum node number has been reduced to a lower
	 * number, but not lower than the minimum number of acceptable nodes.
	 * 
	 * @param request the job request
	 * @return true if it will fit, false if not
	 */
	private boolean acceptableRequest(ResourceRequest request) {
		ResourceUsage currentUsage = connection.getResourceUsage();
		boolean instanceQuotaOK = false;
		boolean coreQuotaOK = false;
		if (instanceQuota == 0) {
			logger.debug("This queue has no instance limits, so this job looks good in this regard.");
			instanceQuotaOK = true;
		} else {
			instanceQuotaOK = (currentUsage.getInstances() + request.getMinNodes() <= instanceQuota);
			
			// Revise down maximum instances if minimum is OK but maximum would exceed the quota
			if (instanceQuotaOK && (currentUsage.getInstances() + request.getMaxNodes() > instanceQuota)) {
				logger.debug("Job's node maximum will not fit, but acceptable range OK. Revising down job size so it will fit.");
				request.setMaxNodes(instanceQuota - currentUsage.getInstances());
			}
		}
		
		if (coreQuota == 0) {
			logger.debug("This queue has no core limits, so this job looks good in this regard.");
			coreQuotaOK = true;
		} else {
			coreQuotaOK = (currentUsage.getCores() + (request.getMinNodes() * request.getMinNodeSize()) <= coreQuota);
			
			// Revise down maximum instances if minimum is OK but maximum would exceed the quota
			if (coreQuotaOK && (currentUsage.getCores() + (request.getMaxNodes() * request.getMinNodeSize()) > coreQuota)) {
				logger.debug("The number of cores requested for each node will exceed the queue limits, but acceptable node range OK. Revising down job size so it will fit.");
				request.setMaxNodes((coreQuota - currentUsage.getCores()) / request.getMinNodeSize());
			}
		}
		
		return instanceQuotaOK && coreQuotaOK;
	}
	
	/**
	 * Shuts down the queue, and all running jobs
	 */
	public void close() {
		isRunning = false;
		logger.debug("Queue shutdown requested!");
		logger.debug("Removing all waiting jobs...");
		clear(); // Clear queued jobs
		
		// Wait for any jobs that are in limbo.
		// The ServerCollection object constructor is currently running for these jobs
		// and cannot be easily interrupted. Therefore this stage is allowed to complete
		// before the jobs are terminated.
		if (!startingJobs.isEmpty()) { 
			logger.debug("There are still jobs in a scheduling/provisioning state.");
			logger.debug("Waiting for these to move into a running state before killing...");
			while (!startingJobs.isEmpty()) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		// Terminate all running jobs (including any that were previously starting
		logger.debug("Killing all running jobs...");
		for (JobStatus job : runningJobs.keySet()) {
			runningJobs.remove(job).close();
		}
		logger.debug("All running jobs killed.");
	}
	
	/**
	 * Creates the required instances for the job to execute, and if successful,
	 * runs the {@link ResourceProvisioningCallback#onSuccess(ServerCollection)} method
	 * of the {@link ResourceRequest} callback via {@link ResourceRequest#doSuccess(ServerCollection)}.
	 * If for some reason resources cannot be allocated, {@link ResourceProvisioningCallback#onFailure(Throwable)}
	 * is called via {@link ResourceRequest#doFailure(Throwable)}.
	 * 
	 * This method is synchronized because is calls {@link OSConnection#doVMGarbageCollection()}, which will
	 * kill any VMs that are in the process of starting. Therefore, this method is NOT thread safe.
	 * 
	 * @param request
	 */
	private synchronized void allocate(final ResourceRequest request) {
		logger.debug("Allocating resources for job \""+request.getGroupName()+"\"...");
		setQueueStatusScheduling(request); // Mark the request as SCHEDULING
		
		// Create a key pair for all the instances being created
		final KeyPair keyPair = connection.generateKeyPair();
		// Determine the server flavour
		final Flavor serverFlavor = connection.getFlavorMinimumCPUs(request.getMinNodeSize());
		// Create a synchronized list of servers (servers are created in parallel)
		final List<Server> allocatedServers = Collections.synchronizedList(new ArrayList<Server>(request.getMaxNodes()));
		
		// Get the list of acceptable availability zones from the request object
		List<String> availabilityZones = Arrays.asList(request.getPreferredAvailabilityZones());
		Iterator<String> azIterator = availabilityZones.iterator();
		String zone = azIterator.next();
		
		// This will search for an AZ with room, and then launch as many as possible in parallel
		while (allocatedServers.size() < request.getMaxNodes()) {
			logger.debug("Trying to allocate resources in availability zone \""+zone+"\"");
			
			// Attempt to allocate a single server in the availability zone
			Server s = allocate(request.getGroupName(), serverFlavor, request.getOsImageId(), keyPair, request.getSecurityGroups(), zone, request.getServerOptions());
			
			// If the server was created successfully, go ahead and try to create the rest here...
			if (s != null) { // Successful creation
				logger.debug("Node successfully created, so trying to allocate remaining resources in this availability zone...");
				allocatedServers.add(s);
				
				int remainingNodes = request.getMaxNodes() - allocatedServers.size();
				Stack<Future<?>> workers = new Stack<Future<?>>();
				final String currentZone = zone;
				for (int i = 0; i < remainingNodes; i++) { // Create the remaining instances in parallel
					workers.push(serverSchedulingThreadPool.submit(new Runnable() {
						public void run() {
							Server s = allocate(request.getGroupName(), serverFlavor, request.getOsImageId(), keyPair, request.getSecurityGroups(), currentZone, request.getServerOptions());
							if (s != null) {
								allocatedServers.add(s);
							}
						}
					}));
				}
				
				// Wait for the server creation attempts to finish
				while (!workers.isEmpty()) {
					try {
						workers.pop().get();
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}
				
				// Check if the allocation fulfilled the request
				// If not, advance to the next availability zone if there are more
				// otherwise, give up.
				if (allocatedServers.size() < request.getMaxNodes() && azIterator.hasNext()) {
					logger.debug("Couldn't fit all resources into availability zone. Moving to the next one.");
					zone = azIterator.next();
				} else if (!azIterator.hasNext()) {
					logger.debug("Couldn't fit all resources into availability zone and availability zone list exhausted. Giving up.");
					break;
				}
				
			} else if (azIterator.hasNext()) { // If we end up here, it means that not even one server was created in the availability zone
				logger.debug("No room in availability zone, so moving to the next one.");
				zone = azIterator.next();
			} else {
				logger.debug("No more availability zones left to try.");
				break;
			}
		}
		
		// Check if the allocation was acceptable given the cloud resources available
		if (allocatedServers.size() >= request.getMinNodes()) {
			logger.debug("Resources successfully allocated.");
			logger.debug("Waiting an additional 60 seconds for all nodes to fully boot. Be patient.");
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			// This thread allows the queue to process other jobs whilst the job executes remotely
			Callable<Void> job = new Callable<Void>() {

				public Void call() {
					setQueueStatusProvisioning(request);
					ServerCollection nodes = new ServerCollection(connection, request.getDefaultLoginUser(), keyPair, request.getLogPath(), allocatedServers.toArray(new Server[allocatedServers.size()]));
					
					// Mark the job as running
					setQueueStatusRunning(request, nodes);
					logger.debug("Job \""+request.getGroupName()+"\" is now running.");
					request.doSuccess(nodes);
					logger.debug("Job \""+request.getGroupName()+"\" has now finished.");
					// Mark the job as completed (remove from all lists)
					setQueueStatusCompleted(request);
					return null;
				}
			};
			
			runningJobFutures.put(request.getId(), runningJobThreadPool.submit(job));
			
		} else { // This code block runs if the job requirements couldn't be satisfied
			logger.debug("Sufficient resources could not be allocated in any availability zone. Killing any nodes that started.");
			Stack<Thread> workers = new Stack<Thread>();
			for (final Server s : allocatedServers) {
				workers.push(new Thread() {
					public void run() {
						connection.terminateServer(s);
					}
				});
				workers.peek().start();
			}
			while (!workers.isEmpty()) {
				try {
					workers.pop().join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			connection.deleteKeyPair(keyPair);
			if (request.doFailure(new InsufficientResourcesException())) { // if this method returns true, the job should be rescheduled
				logger.debug("Rescheduling job.");
				try {
					ResourceQueue.this.putFirst(request);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				setQueueStatusCompleted(request); // Remove request from job lists
			}
		}
	}
	
	/**
	 * Allocates a single server
	 * 
	 * @param serverGroup the server name prefix
	 * @param f VM flavour
	 * @param imageId OS image ID
	 * @param keyPair SSH key pair
	 * @param securityGroups array of security groups to apply
	 * @param availabilityZone availability zone in which the VM should be created
	 * @param serverOptions additional server options
	 * @return a Server object
	 */
	private Server allocate(String serverGroup, Flavor f, String imageId, KeyPair keyPair, String[] securityGroups, String availabilityZone, CreateServerOptions...serverOptions) {
		Server s = connection.createServer(serverGroup+"-"+UUID.randomUUID().toString(),
				imageId,
				f,
				availabilityZone,
				securityGroups,
				keyPair,
				maxServerCreationAttempts,
				serverOptions);
		if (s == null) {
			return null;
		}
		logger.debug("Checking if server was created successfully...");
		if (serverCreatedSuccessfully(s)) {
			logger.debug("It was!");
			return s;
		}
		logger.debug("It wasn't.");
		connection.terminateServer(s);
		
		return null;
	}
	
	/**
	 * Checks if the server was created successfully. This method will block
	 * until the server status is known or until a timeout threshold is reached.
	 * 
	 * @see ResourceQueue#getServerSchedulingTimeLimit()
	 * @param s the Server object
	 * @return true if successfully created
	 */
	private boolean serverCreatedSuccessfully(Server s) {
		long startTime = System.currentTimeMillis();
		while (System.currentTimeMillis() - startTime < getServerSchedulingTimeLimit()) {
			Status currentServerStatus = connection.checkServerStatus(s);
			if (currentServerStatus == null) {
				return false;
			} else if ( currentServerStatus == Status.ACTIVE) {
				return true;
			} else if (currentServerStatus == Status.ERROR) {
				return false;
			}
			try {
				Thread.sleep(5000); // 5 second sleep
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		logger.debug("Server took too long to start. Assuming it never will.");
		return false;
	}

	/**
	 * Gets the server scheduling time limit
	 * @return the server scheduling time limit
	 */
	public Long getServerSchedulingTimeLimit() {
		return serverSchedulingTimeLimit;
	}

	/**
	 * Sets the server scheduling time limit
	 * @param serverSchedulingTimeLimit
	 */
	public void setServerSchedulingTimeLimit(Long serverSchedulingTimeLimit) {
		this.serverSchedulingTimeLimit = serverSchedulingTimeLimit;
	}

	/**
	 * Gets the instance quota
	 * @return maximum allowed instances
	 */
	public int getInstanceQuota() {
		return instanceQuota;
	}

	/**
	 * Sets the instance quota
	 * @param instanceQuota maximum allowed instances
	 */
	public void setInstanceQuota(int instanceQuota) {
		this.instanceQuota = instanceQuota;
	}

	/**
	 * Gets the core quota
	 * @return maximum allowed cores
	 */
	public int getCoreQuota() {
		return coreQuota;
	}

	/**
	 * Sets the core quota
	 * @param coreQuota maximum allowed cores
	 */
	public void setCoreQuota(int coreQuota) {
		this.coreQuota = coreQuota;
	}

	/**
	 * Gets the maximum server creation attempts (in the event of nova errors)
	 * @return maximum server creation attempts
	 */
	public int getMaxServerCreationAttempts() {
		return maxServerCreationAttempts;
	}

	/**
	 * Sets the maximum server creation attempts (in the event of nova errors)
	 * @param maxServerCreationAttempts maximum server creation attempts
	 */
	public void setMaxServerCreationAttempts(int maxServerCreationAttempts) {
		this.maxServerCreationAttempts = maxServerCreationAttempts;
	}
	
	/**
	 * Kills any job running beyond its time limit
	 * 
	 * @see JobStatus#timeLimitExceeded()
	 */
	private void killJobsOverTimeLimit() {
		Stack<Thread> workers = new Stack<Thread>();
		for (final JobStatus job : runningJobs.keySet()) {
			if (job.timeLimitExceeded()) {
				workers.push(new Thread() {
					public void run() {
						logger.debug("Job \""+job.getName()+"\" went over the execution time limit. Killing.");
						runningJobs.remove(job).close();
					}
				});
				workers.peek().start();
			}
		}
		
		while (!workers.isEmpty()) {
			try {
				workers.pop().join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Returns a set of all job in the queue, including those currently running
	 * @return set of all jobs in the queue
	 */
	public Set<JobStatus> getJobsInQueue() {
		TreeSet<JobStatus> unprocessedJobs = new TreeSet<JobStatus>();
		for (ResourceRequest req : this) {
			unprocessedJobs.add(requestToJobStatus(req, QueueStatus.QUEUED, 0L));
		}
		
		return new ImmutableSet.Builder<JobStatus>().addAll(unprocessedJobs).addAll(startingJobs).addAll(runningJobs.keySet()).build();
	}
	
	/**
	 * Kills a job based on its numerical identifier
	 * @see JobStatus#getId()
	 * @param jobId the ID of the job to kill
	 */
	public void killJob(int jobId) {
		logger.debug("Killing job ID "+jobId);
		// First, check if the job is still queuing
		// (just remove it from the queue)
		for (ResourceRequest req : this) {
			if (req.getId() == jobId) {
				remove(req);
				return;
			}
		}
		
		// Then check if the job is currently running
		// (shutdown the instances)
		for (JobStatus job : getJobsInQueue()) {
			if (job.getId() == jobId && job.getStatus().equals(ResourceQueue.QueueStatus.RUNNING.toString())) {
				ServerCollection serverCollection = runningJobs.remove(job);
				serverCollection.close();
				runningJobFutures.remove(jobId).cancel(true); // Forcefully terminate the thread
				return;
			}
		}
		logger.debug("Job ID not found, so nothing killed. (could be scheduling or provisioning)");
	}
	
	/**
	 * Converts a {@link ResourceRequest} object to a {@link JobStatus} object
	 * 
	 * @param request the request to convert
	 * @param status the status of the job
	 * @param startTime the starting time of the job in milliseconds
	 * @return a JobStatus object
	 */
	private JobStatus requestToJobStatus(ResourceRequest request, QueueStatus status, Long startTime) {
		JobStatus job = new JobStatus();
		job.setName(request.getGroupName());
		job.setId(request.getId());
		job.setStatus(status.toString());
		job.setStartTime(startTime);
		job.setTimeLimit(request.getJobMaximumWalltime());
		return job;
	}
	
	/**
	 * Sets the status of a request to SCHEDULING
	 * 
	 * @param req request to set
	 */
	private void setQueueStatusScheduling(ResourceRequest req) {
		// Avoid unnecessary shuffling of job statuses (check if already in the desired state)
		for (JobStatus job : startingJobs) {
			if (job.getId() == req.getId() && job.getStatus().equals(QueueStatus.SCHEDULING.toString())) {
				return;
			}
		}
		clearJobStatus(req);
		
		startingJobs.add(requestToJobStatus(req, QueueStatus.SCHEDULING, 0L));
	}
	
	/**
	 * Sets the status of a request to PROVISIONING
	 * 
	 * @param req request to set
	 */
	private void setQueueStatusProvisioning(ResourceRequest req) {
		// Avoid unnecessary shuffling of job statuses (check if already in the desired state)
		for (JobStatus job : startingJobs) {
			if (job.getId() == req.getId() && job.getStatus().equals(QueueStatus.PROVISIONING.toString())) {
				return;
			}
		}
		clearJobStatus(req);
		
		startingJobs.add(requestToJobStatus(req, QueueStatus.PROVISIONING, 0L));
	}
	
	/**
	 * Sets the status of a request to RUNNING
	 * 
	 * @param req request to set
	 * @param nodes nodes the job is running on
	 */
	private void setQueueStatusRunning(ResourceRequest req, ServerCollection nodes) {
		clearJobStatus(req);
		
		JobStatus job = requestToJobStatus(req, QueueStatus.RUNNING, System.currentTimeMillis());
		runningJobs.put(job, nodes);
	}
	
	/**
	 * Sets the status of a request to COMPLETED
	 * The current implementation just clears the job from all lists
	 * and is equivalent to {@link ResourceQueue#clearJobStatus(ResourceRequest)}.
	 * 
	 * @param req request to set
	 */
	private void setQueueStatusCompleted(ResourceRequest req) {
		clearJobStatus(req);
	}
	
	/**
	 * Clears the job from all status lists
	 * 
	 * @param req request to clear
	 */
	private void clearJobStatus(ResourceRequest req) {
		// Clear from running jobs list
		for (JobStatus job : runningJobs.keySet()) {
			if (job.getId() == req.getId()) {
				runningJobs.remove(job);
				return;
			}
		}
		
		// Clear from starting jobs
		Iterator<JobStatus> iterator = startingJobs.iterator();
		while (iterator.hasNext()) {
			if (iterator.next().getId() == req.getId()) {
				iterator.remove();
				return;
			}
		}
	}
	
}
