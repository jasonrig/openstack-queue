package jrigby.openstack_queue.server;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.http.HttpResponseException;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.openstack.nova.v2_0.NovaApi;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;
import org.jclouds.openstack.nova.v2_0.domain.KeyPair;
import org.jclouds.openstack.nova.v2_0.domain.Server;
import org.jclouds.openstack.nova.v2_0.domain.Server.Status;
import org.jclouds.openstack.nova.v2_0.domain.ServerCreated;
import org.jclouds.openstack.nova.v2_0.domain.zonescoped.AvailabilityZone;
import org.jclouds.openstack.nova.v2_0.extensions.KeyPairApi;
import org.jclouds.openstack.nova.v2_0.features.ServerApi;
import org.jclouds.openstack.nova.v2_0.options.CreateServerOptions;
import org.jclouds.ssh.jsch.config.JschSshClientModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;

/**
 * Provides a simplified abstraction to the OpenStack API
 * 
 * @author jason
 *
 */
public class OSConnection implements Closeable {
	
	private final static Logger logger = LoggerFactory.getLogger(OSConnection.class);
	
	private NovaApi novaApi;
	private ComputeService computeService;
	private String DEFAULT_ZONE;
	
	/**
	 * Establishes a connection the OpenStack with the given credentials
	 * 
	 * @param credentials
	 * @param endpointURL e.g. https://keystone.rc.nectar.org.au:5000/v2.0/
	 */
	public OSConnection(OSCredentials credentials, String endpointURL) {
		Iterable<Module> modules = ImmutableSet.<Module>of(new SLF4JLoggingModule(),
				new JschSshClientModule());

        String provider = "openstack-nova";
        String identity = credentials.getTenantId()+":"+credentials.getUserName();
        String credential = credentials.getPassword();

        ContextBuilder contextBuilder = ContextBuilder.newBuilder(provider)
                .endpoint(endpointURL)
                .credentials(identity, credential)
                .modules(modules);
        
        novaApi = contextBuilder.buildApi(NovaApi.class);
        computeService = contextBuilder.buildView(ComputeServiceContext.class).getComputeService();
        DEFAULT_ZONE = novaApi.getConfiguredZones().iterator().next();
	}
	
	/**
	 * Gets all configured availability zones
	 * @return a set of availability zones
	 */
	public Set<? extends AvailabilityZone> getAvailabilityZones() {
		logger.debug("Querying nova for availability zones...");
		return novaApi.getAvailabilityZoneApi(DEFAULT_ZONE).list().toSet();
	}
	
	/**
	 * Gets all configured VM flavours
	 * @return a set of VM flavours
	 */
	@SuppressWarnings("unchecked")
	public Set<Flavor> getFlavors() {
		logger.debug("Querying nova for VM flavours...");
		return (Set<Flavor>) novaApi.getFlavorApiForZone(DEFAULT_ZONE).listInDetail().concat().toSet();
	}
	
	/**
	 * Gets the VM flavour that has the smallest number of CPUs to accomodate
	 * the desired number.
	 * @param nCPUs number of CPUs
	 * @return the smallest acceptable flavour
	 */
	public Flavor getFlavorMinimumCPUs(int nCPUs) {
		logger.debug("Determining smallest VM flavour for requested CPU count...");
		Flavor currentFlavor = null;
		for (Flavor f : getFlavors()) {
			if (currentFlavor == null && f.getVcpus() >= nCPUs) {
				currentFlavor = f;
				continue;
			} else if (currentFlavor != null && f.getVcpus() >= nCPUs && f.getVcpus() < currentFlavor.getVcpus()) {
				currentFlavor = f;
			}
		}
		
		return currentFlavor;
	}
	
	/**
	 * Gets the ComputeService object provided by Apache jClouds
	 * 
	 * @return the ComputeService object
	 */
	public ComputeService getComputeService() {
		return computeService;
	}
	
	/**
	 * Gets the metadata associated with a server
	 * 
	 * @param s Server
	 * @return node metadata
	 */
	public NodeMetadata getNodeMetadata(Server s) {
		return getComputeService().getNodeMetadata(DEFAULT_ZONE+"/"+s.getId());
	}
	
	/**
	 * Gets the OpenStack server API
	 * 
	 * @return the server API
	 */
	protected ServerApi getServerApi() {
		return novaApi.getServerApiForZone(DEFAULT_ZONE);
	}
	
	/**
	 * Gets the OpenStack key pair API
	 * 
	 * @return the key pair API
	 */
	protected KeyPairApi getKeyPairApi() {
		return novaApi.getKeyPairExtensionForZone(DEFAULT_ZONE).get();
	}
	
	/**
	 * Generates an SSH key pair to inject when creating a VM
	 * 
	 * @return the key pair
	 */
	public KeyPair generateKeyPair() {
		logger.debug("Generating key pair...");
		return getKeyPairApi().create(UUID.randomUUID().toString());
	}
	
	/**
	 * Deletes a key pair
	 * 
	 * @param keyPair
	 */
	public void deleteKeyPair(KeyPair keyPair) {
		logger.debug("Deleting key pair...");
		getKeyPairApi().delete(keyPair.getName());
	}
	
	/**
	 * Creates a VM instance. The VM created using this instance is not
	 * guaranteed to have been created successfully. It is up to the programmer
	 * to subsequently check that the VM started correctly.
	 * 
	 * @see #checkServerStatus(Server)
	 * @param name Server name
	 * @param imageId image identifier to use for the new VM
	 * @param flavor machine flavour
	 * @param availabilityZone availablity zone in which to create the server
	 * @param securityGroups security groups to apply to the new server
	 * @param keyPair key pair to inject
	 * @param retryCount number of times to attempt to create this server if an error is encountered
	 * @param serverOptions additional server options
	 * @return a Server object representing the VM being created
	 */
	public Server createServer(String name, String imageId, Flavor flavor, String availabilityZone, String[] securityGroups, KeyPair keyPair, int retryCount, CreateServerOptions...serverOptions) {
		logger.debug("Attempting to create server \""+name+"\" in availability zone \""+availabilityZone+"\"...");
		
		CreateServerOptions[] options = new CreateServerOptions[serverOptions.length + 1];
		options[0] = CreateServerOptions.Builder
				.availabilityZone(availabilityZone)
				.securityGroupNames(securityGroups)
				.keyPairName(keyPair.getName());
		for (int i = 0; i < serverOptions.length; i++) {
			options[i+1] = serverOptions[i];
		}
		
		ServerCreated serverCreated = null;
		try {
			serverCreated = getServerApi().create(name, imageId, flavor.getId(), options);
		} catch (HttpResponseException e) {
			logger.warn("Nova returned something unexpected while creating the VM!");
			logger.warn("Nova said: "+e.getMessage());
			
			if (retryCount > 0) {
				logger.warn("Will retry in 5 seconds.");
				try {
					Thread.sleep(5000); // 5 second sleep
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				return createServer(name, imageId, flavor, availabilityZone, securityGroups, keyPair, (retryCount - 1), serverOptions);
			} else {
				logger.warn("Giving up.");
				return null;
			}
			
		}
		
		return (serverCreated != null)?getServerApi().get(serverCreated.getId()):null;
				
	}
	
	/**
	 * Kills the given server
	 * 
	 * @param server server to kill
	 */
	public void terminateServer(Server server) {
		logger.debug("Deleting server \""+server.getName()+"\"...");
		final long timeout = 120000L;
		long startTime = System.currentTimeMillis();
		
		getComputeService().destroyNode(DEFAULT_ZONE+"/"+server.getId());
		
		while (serverExists(server)) {
			// Check for retry timeout
			if ((System.currentTimeMillis() - startTime) > timeout) {
				logger.debug("Server still not deleted! Trying again...");
				getComputeService().destroyNode(DEFAULT_ZONE+"/"+server.getId());
				startTime = System.currentTimeMillis();
			}
			
			try {
				Thread.sleep(5000); // 5 second sleep
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * Checks if a server exists regardless of state.
	 * @param server the server to check
	 * @return true if it exists
	 */
	public boolean serverExists(Server server) {
		try {
			getComputeService().getNodeMetadata(DEFAULT_ZONE+"/"+server.getId()).getStatus();
			return true;
		} catch (NullPointerException e) {
			return false;
		}
	}
	
	/**
	 * Checks the status of a server
	 * @param server
	 * @return the status or null if the server doesn't exist
	 */
	public Status checkServerStatus(Server server) {
		Server s = getServerApi().get(server.getId());
		if (s != null) {
			return s.getStatus();
		} else {
			return null;
		}
	}
	
	/**
	 * Gets a set of all servers
	 * @return a set of servers
	 */
	@SuppressWarnings("unchecked")
	public Set<Server> getServerList() {
		return (Set<Server>) getServerApi().listInDetail().concat().toSet();
	}
	
	/**
	 * Checks the current cloud utilisation
	 * @return the current cloud utilisation
	 */
	public ResourceUsage getResourceUsage() {
		logger.debug("Querying nova for current resource usage...");
		ResourceUsage usage = new ResourceUsage();
		Set<Server> servers = getServerList();
		usage.setServers(servers);
		usage.setInstances(servers.size());
		int cores = 0;
		for (Server s : servers) {
			Flavor f = novaApi.getFlavorApiForZone(DEFAULT_ZONE).get(s.getFlavor().getId());
			cores += f.getVcpus();
		}
		usage.setCores(cores);
		logger.debug("Currently "+usage.getInstances()+" instances and "+usage.getCores()+" cores are being used.");
		
		return usage;
	}

	/**
	 * Closes the API
	 */
	public void close() throws IOException {
		novaApi.close();
	}

}
