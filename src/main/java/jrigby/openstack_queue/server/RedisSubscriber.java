package jrigby.openstack_queue.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPubSub;

/**
 * Receives redis messages and directs them to the {@link QueueCommandProcessor}
 * 
 * @author jason
 *
 */
public class RedisSubscriber extends JedisPubSub {
	
	private final static Logger logger = LoggerFactory.getLogger(RedisSubscriber.class);
	
	private QueueCommandProcessor commandProcessor;
	
	/**
	 * Sets up the object to forward all requests to the given {@link QueueCommandProcessor}
	 * @param commandProcessor
	 */
	public RedisSubscriber(QueueCommandProcessor commandProcessor) {
		this.commandProcessor = commandProcessor;
	}
	
	/**
	 * Called by redis when a message arrives. The message is then routed
	 * as either a job submission request, or a queue admin request.
	 */
	@Override
	public void onMessage(String channel, String message) {
		logger.debug("Message received on channel \""+channel+"\". Processing...");
		if (channel.equals(RedisConstants.REDIS_JOB_CHANNEL)) {
			commandProcessor.processCommand(message, QueueCommandProcessor.CommandType.JOB_SUBMIT);
		} else if (channel.equals(RedisConstants.REDIS_ADMIN_CHANNEL)) {
			if (message.equals("shutdown")) { // "shutdown" is a universal command that stops the server
				logger.debug("Shutdown initiated!");
				logger.debug("Unsubscribing from redis message queue...");
				unsubscribe();
				logger.debug("Unsubscribed.");
			} else {
				commandProcessor.processCommand(message, QueueCommandProcessor.CommandType.ADMIN);
			}
		}
		logger.debug("Message processed.");
	}

	// === The following methods are not implemented ===
	/**
	 * Not implemented
	 */
	@Override
	public void onPMessage(String pattern, String channel, String message) {
	}

	/**
	 * Not implemented
	 */
	@Override
	public void onPSubscribe(String pattern, int subscribedChannels) {
	}

	/**
	 * Not implemented
	 */
	@Override
	public void onPUnsubscribe(String pattern, int subscribedChannels) {
	}

	/**
	 * Not implemented
	 */
	@Override
	public void onSubscribe(String channel, int subscribedChannels) {
	}

	/**
	 * Not implemented
	 */
	@Override
	public void onUnsubscribe(String channel, int subscribedChannels) {
	}

}
