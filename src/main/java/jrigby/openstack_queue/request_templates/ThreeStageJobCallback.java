package jrigby.openstack_queue.request_templates;

import jrigby.openstack_queue.server.ResourceProvisioningCallback;
import jrigby.openstack_queue.server.ServerCollection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partial implementation of a three-stage callback. These stages are namely:
 * bootstrap, execute and cleanup.
 * 
 * @author jason
 *
 */
public abstract class ThreeStageJobCallback implements ResourceProvisioningCallback, ThreeStageJobInterface {

	private final static Logger logger = LoggerFactory.getLogger(ThreeStageJobCallback.class);
	
	public ThreeStageJobCallback() {
		
	}

	/**
	 * Executes the three stages if the servers were successfully allocated
	 */
	public void onSuccess(final ServerCollection nodes) {
		try {
			logger.debug("Executing bootstrap script...");
			bootstrap(nodes);
			logger.debug("Bootstrap script completed.");
			logger.debug("Executing execute script...");
			execute(nodes);
			logger.debug("Execute script completed.");
			logger.debug("Cleanup bootstrap script...");
			cleanup(nodes);
			logger.debug("Cleanup script completed.");
				
			logger.debug("Terminating nodes...");
			nodes.close();
			logger.debug("Nodes terminated.");
			
		/* This exception is thrown when the ServerCollection is closed
		 * externally (e.g. the job is killed) and an operation is requested
		 * post closure. Once the ServerCollection is closed, all associated
		 * VM instances are terminated.
		 */
		} catch (IllegalStateException e) {
			if (nodes.isOpen()) {
				nodes.close();
			} else {
				return;
			}
			e.printStackTrace();
			logger.error("Error executing job. Nodes now closed.");
		}
	}

}
