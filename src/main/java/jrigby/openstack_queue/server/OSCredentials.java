package jrigby.openstack_queue.server;

/**
 * Credentials to be passed to the {@link OSConnection} object
 * 
 * @author jason
 *
 */
public class OSCredentials {
    private String tenantId;
    private String userName;
    private String password;
    
    /**
     * Parameterless constructor.
     * Tenant ID, user name and password must be set later.
     */
    public OSCredentials() {
    	tenantId = "";
    	userName = "";
    	password = "";
    }
    
    /**
     * Fully specified OpenStack credentials constructor.
     * 
     * @param tenantId Tenant or Project ID
     * @param userName user name
     * @param password password
     */
    public OSCredentials(String tenantId, String userName, String password) { 
    	this.tenantId = tenantId;
    	this.userName = userName;
    	this.password = password;
    }
    
    /**
     * Gets the tenant ID
     * @return tenant ID
     */
	public String getTenantId() {
		return tenantId;
	}
	
	/**
	 * Sets the tenant ID
	 * @param tenantId Tenant or Project ID
	 */
	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}
	
	/**
	 * Gets the user name
	 * @return user name
	 */
	public String getUserName() {
		return userName;
	}
	
	/**
	 * Sets the user name
	 * @param userName user name
	 */
	public void setUserName(String userName) {
		this.userName = userName;
	}
	
	/**
	 * Gets the password
	 * @return password
	 */
	public String getPassword() {
		return password;
	}
	
	/**
	 * Sets the password
	 * @param password password
	 */
	public void setPassword(String password) {
		this.password = password;
	}
}
