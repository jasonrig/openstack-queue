package jrigby.openstack_queue.server;

/**
 * Thrown when there are insufficient cloud resources to
 * fulfill the request. This is distinct from {@link ResourcesPermanentlyExceededException}
 * and {@link ResourcesTemporarilyExceededException} as these
 * refer to the resource quotas.
 * 
 * @author jason
 *
 */
public class InsufficientResourcesException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 914571820217025370L;

}
