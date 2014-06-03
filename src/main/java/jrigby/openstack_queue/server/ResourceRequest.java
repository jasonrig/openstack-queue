package jrigby.openstack_queue.server;

import org.jclouds.openstack.nova.v2_0.domain.zonescoped.AvailabilityZone;
import org.jclouds.openstack.nova.v2_0.options.CreateServerOptions;

/**
 * Specifies a job to execute that is submitted to
 * a {@link ResourceQueue}.
 * 
 * @author jason
 *
 */
public class ResourceRequest {
	
	private static int nextId = 0;
	
	private int requestId;
	private String groupName;
	private int minNodes;
	private int maxNodes;
	private int minNodeSize;
	private String osImageId;
	private String defaultLoginUser;
	private String logPath;
	private String[] securityGroups;
	private String[] preferredAvailabilityZones;
	private CreateServerOptions[] serverOptions;
	private Long jobMaximumWalltime;
	
	protected ResourceProvisioningCallback callback;
	
	/**
	 * Initialises the request with default values, most of which are defined in
	 * a configuration file.
	 * 
	 * @see ServerSettings
	 * @param serverGroupName the name given to the group of servers (aka job name)
	 * @param callback called once servers allocation completes or fails.
	 */
	public ResourceRequest(String serverGroupName, ResourceProvisioningCallback callback) {
		requestId = nextId;
		nextId++;
		
		this.callback = callback;
		groupName = serverGroupName;
		minNodes = 1;
		maxNodes = 1;
		minNodeSize = 1;
		jobMaximumWalltime = 0L; // 0 = unlimited
		osImageId = ServerSettings.getInstance().getOsDefaultVMImage();
		defaultLoginUser = ServerSettings.getInstance().getOsDefaultVMImageLogin();
		preferredAvailabilityZones = ServerSettings.getInstance().getOsDefaultAvailabilityZonePriority();
		securityGroups = ServerSettings.getInstance().getOsDefaultSecurityGroups();
		serverOptions = new CreateServerOptions[0];
	}
	
	/**
	 * Triggers the {@link ResourceProvisioningCallback#onSuccess(ServerCollection)} method.
	 * @param nodes allocated nodes
	 */
	public void doSuccess(ServerCollection nodes) {
		callback.onSuccess(nodes);
	}
	
	/**
	 * Triggers the {@link ResourceProvisioningCallback#onFailure(Throwable)} method.
	 * 
	 * @param e the exception that resulted from the failure
	 * @return true if the job is to be rescheduled, false if not
	 */
	public boolean doFailure(Throwable e) {
		return callback.onFailure(e);
	}

	/**
	 * Gets the minimum number of acceptable nodes
	 * @return minimum acceptable nodes
	 */
	public int getMinNodes() {
		return minNodes;
	}

	/**
	 * Sets the minimum number of nodes required. If this is greater
	 * than the current value of maxNodes, then maxNodes is set to the
	 * same value as minNodes.
	 * 
	 * @param minNodes minimum nodes required
	 */
	public void setMinNodes(int minNodes) {
		this.minNodes = minNodes;
		if (minNodes > maxNodes) {
			maxNodes = minNodes;
		}
	}

	/**
	 * Gets the maximum number of acceptable nodes. This is the
	 * target of the {@link ResourceQueue}.
	 * @return maximum acceptable nodes
	 */
	public int getMaxNodes() {
		return maxNodes;
	}

	/**
	 * Set the maximum number of nodes required. If maxNodes is less
	 * than this number, then minNodes is set to the same value as
	 * MaxNodes.
	 * 
	 * @param maxNodes maximum nodes required
	 */
	public void setMaxNodes(int maxNodes) {
		this.maxNodes = maxNodes;
		if (maxNodes < minNodes) {
			minNodes = maxNodes;
		}
	}

	/**
	 * Get the minimum node size
	 * 
	 * @return smallest number of CPUs acceptable
	 */
	public int getMinNodeSize() {
		return minNodeSize;
	}

	/**
	 * Sets the minimum node size
	 * 
	 * @param minNodeSize smallest number of CPUs acceptable
	 */
	public void setMinNodeSize(int minNodeSize) {
		this.minNodeSize = minNodeSize;
	}

	/**
	 * Gets the ID of the OpenStack VM image to use
	 * 
	 * @return VM image ID
	 */
	public String getOsImageId() {
		return osImageId;
	}

	/**
	 * Sets the ID of the OpenStack VM image to use
	 * 
	 * @param osImageId VM image ID
	 */
	public void setOsImageId(String osImageId) {
		this.osImageId = osImageId;
	}

	/**
	 * Gets an array of availability zones to try when allocating the resources
	 * 
	 * @return array of availability zones
	 */
	public String[] getPreferredAvailabilityZones() {
		return preferredAvailabilityZones;
	}

	/**
	 * Sets the acceptable availability zones
	 * 
	 * @param preferredAvailabilityZones array of availability zones
	 */
	public void setPreferredAvailabilityZones(String...preferredAvailabilityZones) {
		this.preferredAvailabilityZones = preferredAvailabilityZones;
	}
	
	/**
	 * Sets the acceptable availability zones
	 * 
	 * @param preferredAvailabilityZones array of availability zones
	 */
	public void setPreferredAvailabilityZones(AvailabilityZone...preferredAvailabilityZones) {
		this.preferredAvailabilityZones = new String[preferredAvailabilityZones.length];
		for (int i = 0; i < preferredAvailabilityZones.length; i++) {
			this.preferredAvailabilityZones[i] = preferredAvailabilityZones[i].getName();
		}
	}
	
	/**
	 * Get the additional server options to use when creating the VMs
	 * for this request
	 * @return an array of CreateServerOptions
	 */
	public CreateServerOptions[] getServerOptions() {
		return serverOptions;
	}

	/**
	 * Sets additional server options to use when creating the VMs
	 * for this request
	 * 
	 * @param serverOptions array of CreateServerOptions
	 */
	public void setServerOptions(CreateServerOptions...serverOptions) {
		this.serverOptions = serverOptions;
	}

	/**
	 * Gets an array of security groups to apply to VMs created for this
	 * request.
	 * 
	 * @return array of security groups
	 */
	public String[] getSecurityGroups() {
		return securityGroups;
	}

	/**
	 * Sets the security groups to apply to VMs created for this request.
	 * 
	 * @param securityGroups an array of security groups
	 */
	public void setSecurityGroups(String...securityGroups) {
		this.securityGroups = securityGroups;
	}

	/**
	 * Gets the group name of this request (aka job name).
	 * It is used as the prefix for servers created for this request.
	 * 
	 * @return the server group name
	 */
	public String getGroupName() {
		return groupName;
	}

	/**
	 * Sets the server group name for this request (aka job name).
	 * It is used as the prefix for servers created for this request.
	 * 
	 * @param groupName the server group name
	 */
	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	/**
	 * Gets the default login user for the VM image when
	 * connecting via SSH
	 * 
	 * @return default login user
	 */
	public String getDefaultLoginUser() {
		return defaultLoginUser;
	}

	/**
	 * Sets the default login user for the VM image when
	 * connecting via SSH
	 * 
	 * @param defaultLoginUser the default login user
	 */
	public void setDefaultLoginUser(String defaultLoginUser) {
		this.defaultLoginUser = defaultLoginUser;
	}

	/**
	 * Gets the path in which to write the server logs
	 * 
	 * @return path to write the server logs
	 */
	public String getLogPath() {
		return logPath;
	}

	/**
	 * Sets the path in which to write the server logs
	 * 
	 * @param logPath path to write the server logs
	 */
	public void setLogPath(String logPath) {
		this.logPath = logPath;
	}

	/**
	 * Gets the maximum walltime the job may consume
	 * @return maximum walltime in milliseconds
	 */
	public Long getJobMaximumWalltime() {
		return jobMaximumWalltime;
	}

	/**
	 * Sets the maximum walltime the job may consume
	 * @param jobMaximumWalltime maximum walltime in milliseconds
	 */
	public void setJobMaximumWalltime(Long jobMaximumWalltime) {
		this.jobMaximumWalltime = jobMaximumWalltime;
	}
	
	/**
	 * Gets the request ID. This is set automatically
	 * by incrementing a static counter when the object is
	 * instantiated. It is not a persistent value and will
	 * reset when the application is restarted.
	 * 
	 * @return the request ID
	 */
	public int getId() {
		return requestId;
	}
}
