package jrigby.openstack_queue.server;

import java.util.Set;

import org.jclouds.openstack.nova.v2_0.domain.Server;

/**
 * Specifies consumed resources.
 * 
 * @see OSConnection#getResourceUsage()
 * @author jason
 *
 */
public class ResourceUsage {
	private int instances;
	private int cores;
	private Set<Server> servers;
	public int getInstances() {
		return instances;
	}
	public void setInstances(int instances) {
		this.instances = instances;
	}
	public int getCores() {
		return cores;
	}
	public void setCores(int cores) {
		this.cores = cores;
	}
	public Set<Server> getServers() {
		return servers;
	}
	public void setServers(Set<Server> servers) {
		this.servers = servers;
	}
	
	
}
