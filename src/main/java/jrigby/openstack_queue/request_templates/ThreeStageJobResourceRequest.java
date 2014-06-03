package jrigby.openstack_queue.request_templates;

import java.io.File;

import jrigby.openstack_queue.server.ResourceRequest;

/**
 * A {@link jrigby.openstack_queue.server.ResourceRequest} that includes the
 * {@link ThreeStageJobCallbackImpl} callback.
 * 
 * @author jason
 *
 */
public class ThreeStageJobResourceRequest extends ResourceRequest {
	
	/**
	 * @param serverGroupName server group name, aka job name
	 * @param bootstrapScript boostrap script
	 * @param executeScript execute script
	 * @param cleanupScript cleanup script
	 * @param resultsPath path to which the results should be copied
	 * @param payloadFiles payload files to copy to the remote hosts
	 */
	public ThreeStageJobResourceRequest(String serverGroupName, String bootstrapScript, String executeScript, String cleanupScript, String resultsPath, File...payloadFiles) {
		super(serverGroupName, new ThreeStageJobCallbackImpl(bootstrapScript, executeScript, cleanupScript, resultsPath, payloadFiles));
	}

}
