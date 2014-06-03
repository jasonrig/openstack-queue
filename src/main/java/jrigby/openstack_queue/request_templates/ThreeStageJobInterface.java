package jrigby.openstack_queue.request_templates;

import jrigby.openstack_queue.server.ServerCollection;

/**
 * Defines the three stages used in a three-stage job
 * @author jason
 *
 */
public interface ThreeStageJobInterface {

	/**
	 * Performs any setup tasks
	 * @param nodes allocated nodes
	 */
	public void bootstrap(ServerCollection nodes);
	
	/**
	 * Performs the actual job
	 * @param nodes allocated nodes
	 */
	public void execute(ServerCollection nodes);
	
	/**
	 * Performs any cleanup activities such as collecting results to be returned
	 * @param nodes allocated nodes
	 */
	public void cleanup(ServerCollection nodes);
	
}
