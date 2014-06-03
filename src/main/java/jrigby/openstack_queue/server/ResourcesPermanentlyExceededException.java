package jrigby.openstack_queue.server;

/**
 * Thrown when the request will never fit within the queue quotas.
 * 
 * @author jason
 *
 */
public class ResourcesPermanentlyExceededException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7419653636384753047L;

}
