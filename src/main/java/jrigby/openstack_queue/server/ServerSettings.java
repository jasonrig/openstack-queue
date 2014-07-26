package jrigby.openstack_queue.server;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * An abstraction from the configuration file that specifies all
 * server settings. The configuration file must be named "openstack-queue.properties"
 * and will be searched for in (a) the current directory, (b) the running user's home
 * directory or, (c) the class path, according to the documentation of
 * the Apache Commons Configuration package.
 * 
 * This class enforces a singleton pattern whereby the object is created only once,
 * and subsequent calls to {@link ServerSettings#getInstance()} return the same
 * reference. This means that the configuration file is only read during the first
 * invocation.
 * 
 * @author jason
 *
 */
public class ServerSettings {
	
	private static ServerSettings obj;
	
	private String redisHost;
	private int redisPort;
	
	private String osKeystoneUrl; // e.g. https://keystone.rc.nectar.org.au:5000/v2.0/
	private String osTenantId;
	private String osUserName;
	private String osPassword;
	private String[] osDefaultAvailabilityZonePriority;
	private String[] osDefaultSecurityGroups;
	private String osDefaultVMImage;
	private String osDefaultVMImageLogin;
	
	// Queue limits
	private int limitsMaxInstances;
	private int limitsMaxCores;
	
	/*
	 * Note that below, the distinction is made between server scheduling and provisioning
	 * Scheduling is defined as: Requesting a VM from OpenStack
	 * Provisioning is defined as: Setting up the running VM for use
	 */
	private int limitsMaxSimultaneousServerScheduling; // At most, how many VMs may be starting at any given time?
	private int limitsMaxServerSchedulingAttempts; // How many times to retry server creation of there is an error
	private long limitsServerSchedulingTimeout; // How long to wait for the VM to be available (ms)
	private int limitsMaxSimultaneousFileCopies; // At most, how many simultaneous file copy operations may occur at any given time?

    private boolean reschedule_if_resources_exhausted; // If AZs are all used up, decide whether to reschedule

	/**
	 * Private constructor that must be called via {@link ServerSettings#getInstance()}
	 */
	private ServerSettings() {
		try {
			loadSettings(new PropertiesConfiguration("openstack-queue.properties"));
		} catch (ConfigurationException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Reloads the configuration file
	 */
	public static void reloadConfigurationFile() {
		obj = new ServerSettings();
	}
	
	/**
	 * Reads the configuration file, loads settings and sets default values if not
	 * defined in the configuration file.
	 * 
	 * Please see the {@link ServerSettings#loadSettings(Configuration)} implementation
	 * for the appropriate fields of the configuration file, or see the example configuration
	 * file included with the distribution.
	 * 
	 * @param c configuration object
	 */
	private void loadSettings(Configuration c) {
		
		this.setRedisHost(c.getString("redis.host", "localhost"));
		this.setRedisPort(c.getInt("redis.port", 6379));
		
		this.setOsKeystoneUrl(c.getString("os.keystone-url"));
		this.setOsTenantId(c.getString("os.tenant-id"));
		this.setOsUserName(c.getString("os.user-name"));
		this.setOsPassword(c.getString("os.password"));
		this.setOsDefaultAvailabilityZonePriority(castListObjectToStringArray(c.getList("os.availabilityzones.default"), new String[] {"nova"}));
		this.setOsDefaultSecurityGroups(castListObjectToStringArray(c.getList("os.security-groups.default"), new String[] {"default"}));
		this.setOsDefaultVMImage(c.getString("os.vm-image.default", ""));
		this.setOsDefaultVMImageLogin(c.getString("os.vm-image-login.default", ""));
		
		this.setLimitsMaxCores(c.getInt("limits.max-cores", 0));
		this.setLimitsMaxInstances(c.getInt("limits.max-instances", 0));
		this.setLimitsMaxServerSchedulingAttempts(c.getInt("limits.max-server-scheduling-attempts", 4));
		this.setLimitsServerSchedulingTimeout(c.getLong("limits.server-scheduling-timeout", 300000L));
		this.setLimitsMaxSimultaneousServerScheduling(c.getInt("limits.max-simultaneous-server-scheduling", 8));
		this.setLimitsMaxSimultaneousFileCopies(c.getInt("limits.max-simultaneous-file-copies", 5));

        this.setRescheduleIfResourcesExhausted(c.getBoolean("queue.reschedule_if_resources_exhausted", true));
	}
	
	/**
	 * Casts a list of objects (returned by {@link Configuration#getList(String)}, for example) to an
	 * array of Strings.
	 * 
	 * @param l list of objects
	 * @param defaultData defaults if the list is empty
	 * @return array of Strings
	 */
	private String[] castListObjectToStringArray(List<Object> l, String[] defaultData) {
		if (l == null || l.isEmpty()) {
			return defaultData;
		}
		List<String> result = new ArrayList<String>(l.size());
		for (Object o : l) {
			result.add((String)o);
		}
		return result.toArray(new String[result.size()]);
	}
	
	
	
	public String getRedisHost() {
		return redisHost;
	}

	private void setRedisHost(String redisHost) {
		this.redisHost = redisHost;
	}

	public int getRedisPort() {
		return redisPort;
	}

	private void setRedisPort(int redisPort) {
		this.redisPort = redisPort;
	}

	public String getOsKeystoneUrl() {
		return osKeystoneUrl;
	}

	private void setOsKeystoneUrl(String osKeystoneUrl) {
		this.osKeystoneUrl = osKeystoneUrl;
	}

	public String getOsTenantId() {
		return osTenantId;
	}

	private void setOsTenantId(String osTenantId) {
		this.osTenantId = osTenantId;
	}
	
	public String getOsUserName() {
		return osUserName;
	}

	private void setOsUserName(String osUserName) {
		this.osUserName = osUserName;
	}

	public String getOsPassword() {
		return osPassword;
	}

	private void setOsPassword(String osPassword) {
		this.osPassword = osPassword;
	}

	public String[] getOsDefaultAvailabilityZonePriority() {
		return osDefaultAvailabilityZonePriority;
	}

	private void setOsDefaultAvailabilityZonePriority(
			String[] osDefaultAvailabilityZonePriority) {
		this.osDefaultAvailabilityZonePriority = osDefaultAvailabilityZonePriority;
	}
	

	public String[] getOsDefaultSecurityGroups() {
		return osDefaultSecurityGroups;
	}

	private void setOsDefaultSecurityGroups(String[] osDefaultSecurityGroups) {
		this.osDefaultSecurityGroups = osDefaultSecurityGroups;
	}
	
	public String getOsDefaultVMImage() {
		return osDefaultVMImage;
	}
	
	private void setOsDefaultVMImage(String osDefaultVMImage) {
		this.osDefaultVMImage = osDefaultVMImage;
	}
	
	public String getOsDefaultVMImageLogin() {
		return osDefaultVMImageLogin;
	}
	
	private void setOsDefaultVMImageLogin(String osDefaultVMImageLogin) {
		this.osDefaultVMImageLogin = osDefaultVMImageLogin;
	}

	public int getLimitsMaxInstances() {
		return limitsMaxInstances;
	}

	private void setLimitsMaxInstances(int limitsMaxInstances) {
		this.limitsMaxInstances = limitsMaxInstances;
	}

	public int getLimitsMaxCores() {
		return limitsMaxCores;
	}

	private void setLimitsMaxCores(int limitsMaxCores) {
		this.limitsMaxCores = limitsMaxCores;
	}

	public int getLimitsMaxSimultaneousServerScheduling() {
		return limitsMaxSimultaneousServerScheduling;
	}

	private void setLimitsMaxSimultaneousServerScheduling(
			int limitsMaxSimultaneousServerScheduling) {
		this.limitsMaxSimultaneousServerScheduling = limitsMaxSimultaneousServerScheduling;
	}

	public int getLimitsMaxServerSchedulingAttempts() {
		return limitsMaxServerSchedulingAttempts;
	}

	private void setLimitsMaxServerSchedulingAttempts(
			int limitsMaxServerSchedulingAttempts) {
		this.limitsMaxServerSchedulingAttempts = limitsMaxServerSchedulingAttempts;
	}

	public long getLimitsServerSchedulingTimeout() {
		return limitsServerSchedulingTimeout;
	}

	private void setLimitsServerSchedulingTimeout(
			long limitsServerSchedulingTimeout) {
		this.limitsServerSchedulingTimeout = limitsServerSchedulingTimeout;
	}

	public int getLimitsMaxSimultaneousFileCopies() {
		return limitsMaxSimultaneousFileCopies;
	}

	private void setLimitsMaxSimultaneousFileCopies(
			int limitsMaxSimultaneousFileCopies) {
		this.limitsMaxSimultaneousFileCopies = limitsMaxSimultaneousFileCopies;
	}

    public boolean rescheduleIfResourcesExhausted() {
        return reschedule_if_resources_exhausted;
    }

    private void setRescheduleIfResourcesExhausted(boolean reschedule_if_resources_exhausted) {
        this.reschedule_if_resources_exhausted = reschedule_if_resources_exhausted;
    }

    /**
	 * Returns an instance of {@link ServerSettings}
	 * @return ServerSettings instance
	 */
	public static ServerSettings getInstance() {
		if (obj == null) {
			obj = new ServerSettings();
		}
		
		return obj;
	}
}
