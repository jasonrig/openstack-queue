package jrigby.openstack_queue.server;

/**
 * Provides callback methods once resources have been created
 * in the cloud.
 * 
 * @author jason
 *
 */
public interface ResourceProvisioningCallback {
	
	/**
	 * Called if the resources have been successfully created.
	 * Classes implementing this interface must take care to close the server collection
	 * in order to free up resources for subsequent requests.
	 * 
	 * @param nodes a collection of all servers created for this request
	 */
	public void onSuccess(ServerCollection nodes);
	
	/**
	 * Called if the resources could not be created.
	 * 
	 * @param e the exception that caused the request to fail
	 * @return true if the job is to be rescheduled (i.e. on a temporary failure) or false if the job is to be abandoned
	 */
	public boolean onFailure(Throwable e);

}
