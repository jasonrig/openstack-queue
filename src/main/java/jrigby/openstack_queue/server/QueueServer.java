package jrigby.openstack_queue.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Program entry point
 * 
 * @author jason
 *
 */
public class QueueServer {
	
	private final static Logger logger = LoggerFactory.getLogger(QueueServer.class);
	
	//TODO: put redis settings in config file
	final public static JedisPool jedisPool = new JedisPool(
			new JedisPoolConfig(),
			ServerSettings.getInstance().getRedisHost(),
			ServerSettings.getInstance().getRedisPort()
			);
	
	/**
	 * Main method
	 * 
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		
		// If not set elsewhere, set the maximum script execution time to 1 year.
		// This is a jclouds limit, but we have created our own mechanism to stop
		// long-running scripts.
		if (System.getProperty("jclouds.compute.timeout.script-complete") == null) {
			logger.debug("Setting jclouds script-complete timeout to a very large number.");
			System.setProperty("jclouds.compute.timeout.script-complete", "31560000000");
		} else {
			logger.debug("jclouds script-complete timeout has been manually set.");
		}
		
		// Establish an OpenStack connection
		OSCredentials credentials = new OSCredentials();
		credentials.setTenantId(ServerSettings.getInstance().getOsTenantId());
		credentials.setUserName(ServerSettings.getInstance().getOsUserName());
		credentials.setPassword(ServerSettings.getInstance().getOsPassword());
		OSConnection cloudConnection = new OSConnection(credentials, ServerSettings.getInstance().getOsKeystoneUrl());
		
		logger.debug("OpenStack connection established.");
		
		// Get a ResourceQueue object
		ResourceQueue queue = new ResourceQueue(cloudConnection);
		
		// Set queue limits
		queue.setInstanceQuota(ServerSettings.getInstance().getLimitsMaxInstances());
		queue.setCoreQuota(ServerSettings.getInstance().getLimitsMaxCores());
		
		// Start the queue thread
		logger.debug("Starting queue!");
		Thread queueThread = queue.startQueue();
		logger.debug("Queue thread started.");
		
		// Start the redis publisher thread to broadcast the queue status
		logger.debug("Starting redis queue status publisher. Interested applications may monitor the queue: "+RedisConstants.REDIS_STATUS_CHANNEL);
		Jedis redisPublisherClient = jedisPool.getResource();
		RedisQueueStatusPublisher queueStatusPublisher = new RedisQueueStatusPublisher(redisPublisherClient, queue);
		Thread queueStatusPublisherThread = queueStatusPublisher.start();
		
		// Start the redis subscriber that received queue commands
		logger.debug("Creating a redis subscription to the job submission channel, "+RedisConstants.REDIS_JOB_CHANNEL);
		logger.debug("Creating a redis subscription to the admin channel, "+RedisConstants.REDIS_ADMIN_CHANNEL);
		Jedis redisSubscriberClient = jedisPool.getResource();
		// Will block here until "shutdown" command received on the admin channel
		redisSubscriberClient.subscribe(new RedisSubscriber(new QueueCommandProcessor(queue)), RedisConstants.REDIS_JOB_CHANNEL, RedisConstants.REDIS_ADMIN_CHANNEL);
		
		// Close down the queue status publisher and wait for the thread to exit
		logger.debug("Stopping queue status publisher...");
		queueStatusPublisher.close();
		queueStatusPublisherThread.join();
		logger.debug("No longer publishing the queue status.");
		
		// Return the redis clients to the pool
		logger.debug("Returning all redis clients to the pool.");
		jedisPool.returnResource(redisSubscriberClient);
		jedisPool.returnResource(redisPublisherClient);
		
		// Shut down the queue thread and wait for it to exit
		logger.debug("Stopping queue...");
		queue.close();
		queueThread.join();
		logger.debug("The queue has now stopped. Goodbye!");
	}

}
