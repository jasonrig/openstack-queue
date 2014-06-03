package jrigby.openstack_queue.server;

/**
 * Thrown when the currently running jobs have reduced the available
 * instances/CPUs such that this job cannot currently run.
 * 
 * @author jason
 *
 */
public class ResourcesTemporarilyExceededException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 67759782833920297L;

}
