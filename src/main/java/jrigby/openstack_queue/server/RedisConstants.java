package jrigby.openstack_queue.server;

/**
 * A set of constants for the redis services
 * 
 * @author jason
 *
 */
public class RedisConstants {
	final public static String REDIS_JOB_CHANNEL="jobqueue-submit";
	final public static String REDIS_ADMIN_CHANNEL="jobqueue-admin";
	final public static String REDIS_STATUS_CHANNEL="jobqueue-status";
	
	private RedisConstants() {
		
	}
}
