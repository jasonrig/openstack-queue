package jrigby.openstack_queue.server;

import java.io.Closeable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.google.gson.Gson;

/**
 * Publishes the state of the queue at a set interval
 * 
 * @author jason
 *
 */
public class RedisQueueStatusPublisher implements Runnable, Closeable {
	
	private final static Logger logger = LoggerFactory.getLogger(RedisQueueStatusPublisher.class);
	
	private boolean isRunning = true;
	private Jedis redisClient;
	private ResourceQueue queue;
	private Long publishInterval = 10000L;

	/**
	 * Sets up the publisher to publish the state of the given queue to the specificed redis server
	 * @param redisClient redis client
	 * @param queue the queue to publish
	 */
	public RedisQueueStatusPublisher(Jedis redisClient, ResourceQueue queue) {
		this.redisClient = redisClient;
		this.queue = queue;
	}

	/**
	 * Loops until the object is closed with the status of the queue being
	 * published at each iteration.
	 */
	public void run() {
		logger.debug("Will publish the state of the queue every "+(publishInterval / 1000)+" seconds.");
		while (isRunning) {
			try {
				Thread.sleep(publishInterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			redisClient.publish(RedisConstants.REDIS_STATUS_CHANNEL, new Gson().toJson(queue.getJobsInQueue()));
		}
	}
	
	/**
	 * Starts the thread
	 * @return the Thread object
	 */
	public Thread start() {
		Thread t = new Thread(this);
		t.start();
		return t;
	}

	/**
	 * Signals for the thread to exit
	 */
	public void close() {
		isRunning = false;
	}

}
