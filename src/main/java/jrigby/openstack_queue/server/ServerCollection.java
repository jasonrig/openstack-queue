package jrigby.openstack_queue.server;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jclouds.compute.ComputeService;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Processor;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.compute.predicates.NodePredicates;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.io.payloads.BaseMutableContentMetadata;
import org.jclouds.io.payloads.InputStreamPayload;
import org.jclouds.openstack.nova.v2_0.domain.KeyPair;
import org.jclouds.openstack.nova.v2_0.domain.Server;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.ssh.SshClient;
import org.jclouds.ssh.SshException;
import org.jclouds.ssh.SshKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A set of servers that have been dedicated to a particular
 * {@link ResourceRequest}. It is backed by an {@link com.google.common.collect.ImmutableSet}
 * and therefore cannot be modified after it is created.
 * 
 * @author jason
 *
 */
public class ServerCollection implements Set<Server>, Closeable {
	
	private final static ExecutorService threadPool = Executors.newCachedThreadPool();
	
	/**
	 * Thread pool that limits the number of concurrent file copy operations.
	 * This setting is defined in a configuration file.
	 * @see ServerSettings
	 */
	private final static ExecutorService fileCopyThreadPool = Executors.newFixedThreadPool(ServerSettings.getInstance().getLimitsMaxSimultaneousFileCopies());
	
	private final static Logger logger = LoggerFactory.getLogger(ServerCollection.class);
	
	// Used to flag if the server collection has been closed
	private boolean isOpen;
	
	private OSConnection connection;
	private KeyPair keyPair;
	private ImmutableSet<Server> serverList;
	private ImmutableSet<NodeMetadata> nodeMetadata;
	private String defaultLoginUser;
	private Map<NodeMetadata, PrintWriter> nodeLoggersStdOut;
	private Map<NodeMetadata, PrintWriter> nodeLoggersStdErr;
	private String logPath;
	
	/**
	 * Creates a ServerCollection backed by an {@link com.google.common.collect.ImmutableSet},
	 * which cannot be modified after its creation.
	 * This constructor also performs some server customisation before they are made available
	 * for use.
	 * 
	 * @param connection the OSConnection from which these servers were created
	 * @param defaultLoginUser the default login user for SSH connections to these servers
	 * @param keyPair the SSH key pair injected when these servers were created
	 * @param logPath the path in which to store the server log files
	 * @param servers an array of server objects
	 */
	public ServerCollection(OSConnection connection, String defaultLoginUser, KeyPair keyPair, String logPath, Server...servers) {
		isOpen = true;
		
		this.connection = connection;
		this.keyPair = keyPair;
		this.defaultLoginUser = defaultLoginUser;
		this.logPath = logPath;
		nodeLoggersStdOut = new HashMap<NodeMetadata, PrintWriter>();
		nodeLoggersStdErr = new HashMap<NodeMetadata, PrintWriter>();
		serverList = new ImmutableSet.Builder<Server>().addAll(Arrays.asList(servers)).build();
		nodeMetadata = null;
		
		// Write private key to file so the nodes can be accessed by the user
		{
			PrintWriter out;
			try {
				File privateKeyFile = new File(logPath+"/private_key");
				if (privateKeyFile.exists()) {
					privateKeyFile.delete();
				}
				logger.debug("Writing private key to "+privateKeyFile.getAbsolutePath()+" so you can SSH into the nodes as they run.");
				out = new PrintWriter(privateKeyFile);
				out.print(keyPair.getPrivateKey());
				out.close();
			} catch (FileNotFoundException e1) {
				logger.warn("Could not write private key to file!");
			}
		}
		
		// Provisioning routine
		Future<?> serverProvisioningTask1 = threadPool.submit(new Runnable() {

			public void run() {
				logger.debug("Exchanging SSH keys...");
				createAndExchangeNodeKeyPairs();
				logger.debug("SSH key exchange complete.");
			}
			
		});
		Future<?> serverProvisioningTask2 = threadPool.submit(new Runnable() {

			public void run() {
				logger.debug("Generating custom /etc/hosts /tmp/hostfile /tmp/headnode...");
				setupHostsFile();
				logger.debug("Custom /etc/hosts /tmp/hostfile /tmp/headnode generation complete.");
			}
			
		});
		try {
			// Wait for customisation to complete
			serverProvisioningTask1.get();
			serverProvisioningTask2.get();
		} catch (InterruptedException e) {
			logger.error("Server provisioning was interrupted!");
			logger.error(e.getMessage());
		} catch (ExecutionException e) {
			logger.error("Server provisioning could not be completed due to an execution exception.");
			logger.error(e.getMessage());
		}
	}
	
	/**
	 * Checks if the {@link ServerCollection} is currently active
	 * 
	 * @return true if the ServerCollection is active
	 */
	public boolean isOpen() {
		return isOpen;
	}
	
	/**
	 * Generates key pairs for inter-node communication and
	 * exchanges them between nodes.
	 */
	private void createAndExchangeNodeKeyPairs() {
		
		// Create the keys
		Map<NodeMetadata, Map<String,String>> keys = new HashMap<NodeMetadata, Map<String,String>>();
		for (NodeMetadata node : getNodeMetadata()) {
			keys.put(node, SshKeys.generate());
		}
		
		Stack<Future<?>> workers = new Stack<Future<?>>();
		// Exchange the keys
		// The same keys go to the root and default user
		// This solves any issues of connectivity when switching between users.
		// Note: jclouds key exchange Statement objects do exist (InstallRSAPrivateKey and AuthorizeRSAPublicKey)
		// but these are a little problematic because they seem to only work for the standard user and not for
		// root. To make things more straightforward, the following are custom implementations that properly
		// exchange the keys and have been tested to work on Ubuntu Trusty.
		for (final NodeMetadata node : keys.keySet()) {
			final String privateKey = keys.get(node).get("private");
			final int nodeIndex = getNodeMetadata().asList().indexOf(node);
			
			workers.push(threadPool.submit(new Callable<Void>() {

				public Void call() throws RunScriptOnNodesException {
					copyFileToNode(nodeIndex, privateKey, "/tmp/id_rsa");
					runScriptOnNode(nodeIndex, "mkdir -p ~/.ssh/ && "
							+ "mv /tmp/id_rsa ~/.ssh/ && "
							+ "chown "+defaultLoginUser+":root ~/.ssh/id_rsa && "
							+ "chmod 600 ~/.ssh/id_rsa &&"
							+ "mkdir -p /root/.ssh/ &&"
							+ "cp ~/.ssh/id_rsa /root/.ssh/id_rsa &&"
							+ "chown root:root /root/.ssh/id_rsa &&"
							+ "chmod 600 /root/.ssh/id_rsa");
					return null;
				}
				
			}));
		}
		
		final StringBuilder publicKeys = new StringBuilder();
		for (NodeMetadata node1 : keys.keySet()) {
				publicKeys.append(keys.get(node1).get("public")+"\n");
		}
		workers.push(threadPool.submit(new Callable<Void>() {

			public Void call() throws RunScriptOnNodesException {
				copyFileToAllNodes(publicKeys.toString(), "/tmp/authorized_keys");
				runScriptOnAllNodes("cat /tmp/authorized_keys >> ~/.ssh/authorized_keys && "
				+ "cat /tmp/authorized_keys >> /root/.ssh/authorized_keys &&"
				+ "rm /tmp/authorized_keys");
				return null;
			}
			
		}));
		
		// Disable strict host key checking
		workers.push(threadPool.submit(new Callable<Void>() {

			public Void call() throws Exception {
				runScriptOnAllNodes("echo \"StrictHostKeyChecking no\" >> /etc/ssh/ssh_config");
				return null;
			}
			
		}));
		
		while (!workers.isEmpty()) {
			try {
				workers.pop().get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * Creates /etc/hosts, /tmp/hostfile and /tmp/headnode
	 * so that host name resolution works, and MPI jobs can run
	 */
	private void setupHostsFile() {
		Stack<Future<?>> workers = new Stack<Future<?>>();
		// Set up /etc/hosts file
		for (int nodeIndex = 0; nodeIndex < this.size(); nodeIndex++) {
			final int idx = nodeIndex;
			workers.push(threadPool.submit(new Callable<Void>() {

				public Void call() {
					String hostsFile = generateHostsFileForNode(idx);
					copyFileToNode(idx, hostsFile, "/tmp/hosts");
					return null;
				}
				
			}));
			
		}
		
		// Set up MPI hostfile
		workers.push(threadPool.submit(new Callable<Void>() {

			public Void call() {
				copyFileToAllNodes(ServerCollection.this.generateMPIHostsFile(), "/tmp/hostfile");
				return null;
			}
			
		}));
		
		// Save a headnode file (so that all nodes know which is the 'head')
		workers.push(threadPool.submit(new Callable<Void>() {

			public Void call() {
				copyFileToAllNodes(ServerCollection.this.getNodeMetadata().asList().get(0).getHostname(), "/tmp/headnode");
				return null;
			}
			
		}));
		
		while (!workers.isEmpty()) { // Let all threads finish
			try {
				workers.pop().get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		
		try {
			runScriptOnAllNodes("mv /tmp/hosts /etc/hosts");
		} catch (RunScriptOnNodesException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Get the key pair used when the servers were created
	 * @return the key pair
	 */
	public KeyPair getKeyPair() {
		return keyPair;
	}
	
	/**
	 * Gets the host name for a node
	 * @param nodeIndex index of node
	 * @return the host name
	 */
	public String hostNameOf(int nodeIndex) {
		return getNodeMetadata().asList().get(nodeIndex).getHostname();
	}
	
	/**
	 * Get {@link org.jclouds.compute.domain.NodeMetadata} objects for the
	 * servers in this set
	 * @return server metadata
	 */
	private ImmutableSet<NodeMetadata> getNodeMetadata() {
		if (nodeMetadata != null) {
			return nodeMetadata;
		}
		
		ImmutableSet.Builder<NodeMetadata> setBuilder = new ImmutableSet.Builder<NodeMetadata>();
		for (Server s : this) {
			setBuilder.add(connection.getNodeMetadata(s));
		}
		return setBuilder.build();
	}
	
	/**
	 * Gets the predicate that represents all servers in this group
	 * @return group predicate
	 */
	protected Predicate<NodeMetadata> getGroupPredicate() {
		String[] ids = new String[size()];
		{
			int i = 0;
			for (NodeMetadata node : getNodeMetadata()) {
				ids[i] = node.getId();
				i++;
			}
		}
		return NodePredicates.withIds(ids);
	}
	
	/**
	 * Creates a {@link org.jclouds.compute.options.RunScriptOptions} object
	 * that includes server login credentials to use when logging in to the
	 * remote node
	 * @return RunScriptOptions containing login credentials
	 */
	public RunScriptOptions getScriptLoginCredentials() {
		return RunScriptOptions.Builder.overrideLoginUser(defaultLoginUser).overrideLoginPrivateKey(keyPair.getPrivateKey());
	}
	
	/**
	 * Get the {@link org.jclouds.compute.ComputeService} for the underlying
	 * {@link OSConnection}.
	 * @return ComputeService object
	 */
	protected ComputeService getComputeService() {
		return connection.getComputeService();
	}
	
	/**
	 * Creates a /etc/hosts file for a particular node
	 * @param nodeIndex node index
	 * @return hosts file
	 */
	private String generateHostsFileForNode(int nodeIndex) {
		NodeMetadata thisNode = getNodeMetadata().asList().get(nodeIndex);
		StringBuilder hostsFile = new StringBuilder();
		hostsFile.append("127.0.0.1 localhost\n");
		hostsFile.append("127.0.1.1 "+thisNode.getHostname());
		for (NodeMetadata node : getNodeMetadata()) {
			if (!node.equals(thisNode)) {
				for (String address : node.getPublicAddresses()) {
				hostsFile.append("\n"+address+" "+node.getHostname());
				}
			}
		}
		
		return hostsFile.toString();
	}
	
	/**
	 * Generates an MPI hostfile suitable for OpenMPI jobs
	 * @return an MPI hostfile
	 */
	private String generateMPIHostsFile() {
		StringBuilder hostsFile = new StringBuilder();
		for (NodeMetadata node : getNodeMetadata()) {
			for (Processor p : node.getHardware().getProcessors()) {
				int procCount = (int)p.getCores();
				for (int i = 0; i < procCount; i++) {
					hostsFile.append(node.getHostname()+"\n");
				}
			}
		}
		return hostsFile.toString();
	}
	
	/**
	 * Runs a script on all nodes
	 * @param script the script to run
	 * @return the results from all nodes on which this script was run
	 * @throws RunScriptOnNodesException
	 */
	public Map<? extends NodeMetadata, ExecResponse> runScriptOnAllNodes(String script) throws RunScriptOnNodesException {
		requireOpenState();
		
		return log(getComputeService().runScriptOnNodesMatching(getGroupPredicate(), script, getScriptLoginCredentials().runAsRoot(true).wrapInInitScript(false)));
	}
	
	/**
	 * Runs a script on all nodes
	 * @param script the script to run
	 * @return the results from all nodes on which this script was run
	 * @throws RunScriptOnNodesException
	 */
	public Map<? extends NodeMetadata, ExecResponse> runScriptOnAllNodes(Statement script) throws RunScriptOnNodesException {
		requireOpenState();
		
		return log(getComputeService().runScriptOnNodesMatching(getGroupPredicate(), script, getScriptLoginCredentials().runAsRoot(true).wrapInInitScript(false)));
	}
	
	/**
	 * Runs a script on all nodes
	 * @param script the script to run
	 * @param options additional options for script execution
	 * @return the results from all nodes on which this script was run
	 * @throws RunScriptOnNodesException
	 */
	public Map<? extends NodeMetadata, ExecResponse> runScriptOnAllNodes(String script, RunScriptOptions options) throws RunScriptOnNodesException {
		requireOpenState();
		
		return log(getComputeService().runScriptOnNodesMatching(getGroupPredicate(), script, options));
	}
	
	/**
	 * Runs a script on all nodes
	 * @param script the script to run
	 * @param options additional options for script execution
	 * @return the results from all nodes on which this script was run
	 * @throws RunScriptOnNodesException
	 */
	public Map<? extends NodeMetadata, ExecResponse> runScriptOnAllNodes(Statement script, RunScriptOptions options) throws RunScriptOnNodesException {
		requireOpenState();
		
		return log(getComputeService().runScriptOnNodesMatching(getGroupPredicate(), script, options));
	}
	
	/**
	 * Runs a script on a node
	 * @param nodeIndex node index
	 * @param script the script to run
	 * @return the result
	 * @throws RunScriptOnNodesException
	 */
	public ExecResponse runScriptOnNode(int nodeIndex, String script) throws RunScriptOnNodesException {
		requireOpenState();
		
		NodeMetadata node = getNodeMetadata().asList().get(nodeIndex);
		return log(node, getComputeService().runScriptOnNode(node.getId(), script, getScriptLoginCredentials().runAsRoot(true).wrapInInitScript(false)));
	}
	
	/**
	 * Runs a script on a node
	 * @param nodeIndex node index
	 * @param script the script to run
	 * @return the result
	 * @throws RunScriptOnNodesException
	 */
	public ExecResponse runScriptOnNode(int nodeIndex, Statement script) throws RunScriptOnNodesException {
		requireOpenState();
		
		NodeMetadata node = getNodeMetadata().asList().get(nodeIndex);
		return log(node, getComputeService().runScriptOnNode(node.getId(), script, getScriptLoginCredentials().runAsRoot(true).wrapInInitScript(false)));
	}
	
	/**
	 * Runs a script on a node
	 * @param nodeIndex node index
	 * @param script the script to run
	 * @param options additional script options
	 * @return the result
	 * @throws RunScriptOnNodesException
	 */
	public ExecResponse runScriptOnNode(int nodeIndex, String script, RunScriptOptions options) throws RunScriptOnNodesException {
		requireOpenState();
		
		NodeMetadata node = getNodeMetadata().asList().get(nodeIndex);
		return log(node, getComputeService().runScriptOnNode(node.getId(), script, options));
	}
	
	/**
	 * Runs a script on a node
	 * @param nodeIndex node index
	 * @param script the script to run
	 * @param options additional script options
	 * @return the result
	 * @throws RunScriptOnNodesException
	 */
	public ExecResponse runScriptOnNode(int nodeIndex, Statement script, RunScriptOptions options) throws RunScriptOnNodesException {
		requireOpenState();
		
		NodeMetadata node = getNodeMetadata().asList().get(nodeIndex);
		return log(node, getComputeService().runScriptOnNode(node.getId(), script, options));
	}
	
	/**
	 * Non-blocking script execution on a remote node
	 * @param nodeIndex node index
	 * @param script the script to run
	 * @return a ListenableFuture object to access the result once the script is complete
	 * @throws RunScriptOnNodesException
	 */
	public ListenableFuture<ExecResponse> submitScriptOnNode(int nodeIndex, String script) throws RunScriptOnNodesException {
		requireOpenState();
		
		NodeMetadata node = getNodeMetadata().asList().get(nodeIndex);
		return log(node, getComputeService().submitScriptOnNode(node.getId(), script, getScriptLoginCredentials().runAsRoot(true).wrapInInitScript(false)));
	}
	
	/**
	 * Non-blocking script execution on a remote node
	 * @param nodeIndex node index
	 * @param script the script to run
	 * @return a ListenableFuture object to access the result once the script is complete
	 * @throws RunScriptOnNodesException
	 */
	public ListenableFuture<ExecResponse> submitScriptOnNode(int nodeIndex, Statement script) throws RunScriptOnNodesException {
		requireOpenState();
		
		NodeMetadata node = getNodeMetadata().asList().get(nodeIndex);
		return log(node, getComputeService().submitScriptOnNode(node.getId(), script, getScriptLoginCredentials().runAsRoot(true).wrapInInitScript(false)));
	}
	
	/**
	 * Non-blocking script execution on a remote node
	 * @param nodeIndex node index
	 * @param script the script to run
	 * @param options additional script options
	 * @return a ListenableFuture object to access the result once the script is complete
	 * @throws RunScriptOnNodesException
	 */
	public ListenableFuture<ExecResponse> submitScriptOnNode(int nodeIndex, String script, RunScriptOptions options) throws RunScriptOnNodesException {
		requireOpenState();
		
		NodeMetadata node = getNodeMetadata().asList().get(nodeIndex);
		return log(node, getComputeService().submitScriptOnNode(node.getId(), script, options));
	}
	
	/**
	 * Non-blocking script execution on a remote node
	 * @param nodeIndex node index
	 * @param script the script to run
	 * @param options additional script options
	 * @return a ListenableFuture object to access the result once the script is complete
	 * @throws RunScriptOnNodesException
	 */
	public ListenableFuture<ExecResponse> submitScriptOnNode(int nodeIndex, Statement script, RunScriptOptions options) throws RunScriptOnNodesException {
		requireOpenState();
		
		NodeMetadata node = getNodeMetadata().asList().get(nodeIndex);
		return log(node, getComputeService().submitScriptOnNode(node.getId(), script, options));
	}
	
	/**
	 * Logs the script output
	 * @param node node from which the results came from
	 * @param data the results
	 * @return the results for further processing
	 */
	private ExecResponse log(NodeMetadata node, ExecResponse data) {
		
		// logPath == null inhibits logging
		if (logPath == null) {
			return data;
		}
		
		PrintWriter out;
		PrintWriter err;
		if (nodeLoggersStdOut.containsKey(node)) {
			out = nodeLoggersStdOut.get(node);
		} else {
			try {
				out = new PrintWriter(new File(logPath+"/"+this.getNodeMetadata().asList().indexOf(node)+"-"+node.getName()+".out.log"));
				out.println("This is the stdout log file for "+node.getHostname()+", "+node.getPublicAddresses().iterator().next());
				nodeLoggersStdOut.put(node, out);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				return data;
			}
		}
		
		if (nodeLoggersStdErr.containsKey(node)) {
			err = nodeLoggersStdErr.get(node);
		} else {
			try {
				err = new PrintWriter(new File(logPath+"/"+this.getNodeMetadata().asList().indexOf(node)+"-"+node.getName()+".err.log"));
				err.println("This is the stderr log file for "+node.getHostname()+", "+node.getPublicAddresses().iterator().next());
				nodeLoggersStdErr.put(node, err);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				return data;
			}
		}
		
		if (!data.getOutput().isEmpty()) {
			out.println(data.getOutput());
			out.flush();
		}
		if (!data.getError().isEmpty()) {
			err.println(data.getError());
			err.flush();
		}
		
		return data;
	}
	
	/**
	 * Logs the script output
	 * @param data the results
	 * @return the result for further processing
	 */
	private Map<? extends NodeMetadata, ExecResponse> log(Map<? extends NodeMetadata, ExecResponse> data) {
		for (NodeMetadata node : data.keySet()) {
			log(node, data.get(node));
		}
		return data;
	}
	
	/**
	 * Logs the script output
	 * @param node node from which the results came from
	 * @param data the results
	 * @return the results for further processing
	 */
	private ListenableFuture<ExecResponse> log(final NodeMetadata node, ListenableFuture<ExecResponse> data) {
		Futures.addCallback(data, new FutureCallback<ExecResponse>() {

			public void onFailure(Throwable arg0) {
				throw new RuntimeException(arg0);
			}

			public void onSuccess(ExecResponse arg0) {
				log(node, arg0);
			}
			
		});
		
		return data;
	}
	
	/**
	 * Copies a file to all nodes
	 * @param file file to copy
	 * @param remoteLocation remote path of file
	 * @throws FileNotFoundException
	 */
	public void copyFileToAllNodes(final File file, final String remoteLocation) throws FileNotFoundException {
		requireOpenState();
		
		Stack<Future<?>> workers = new Stack<Future<?>>();
		for (final NodeMetadata node : getNodeMetadata()) {
			workers.push(copyFileToNodeAsync(getNodeMetadata().asList().indexOf(node), file, remoteLocation));
		}
		
		while (!workers.isEmpty()) {
			try {
				workers.pop().get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Copies a file to all nodes
	 * @param content content to copy
	 * @param remoteLocation remote path of file
	 */
	public void copyFileToAllNodes(final String content, final String remoteLocation) {
		requireOpenState();
		
		Stack<Future<?>> workers = new Stack<Future<?>>();
		for (final NodeMetadata node : getNodeMetadata()) {
			workers.push(copyFileToNodeAsync(getNodeMetadata().asList().indexOf(node), content, remoteLocation));
		}
		
		while (!workers.isEmpty()) {
			try {
				workers.pop().get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Asynchronous version of {@link ServerCollection#copyFileToNode(int, File, String)}
	 * 
	 * @see ServerCollection#copyFileToNode(int, File, String)
	 * @param nodeIndex node index
	 * @param file file to copy
	 * @param remoteLocation remote path of file
	 * @return Future object to access result once completed
	 */
	public Future<?> copyFileToNodeAsync(final int nodeIndex, final File file, final String remoteLocation) {
		return threadPool.submit(new Callable<Void>() {

			public Void call() throws FileNotFoundException {
				copyFileToNode(nodeIndex, file, remoteLocation);
				return null;
			}
			
		});
	}
	
	/**
	 * Copies a file to a node
	 * @param nodeIndex node index
	 * @param file file to copy
	 * @param remoteLocation remote path of file
	 * @throws FileNotFoundException
	 */
	public void copyFileToNode(final int nodeIndex, final File file, final String remoteLocation) throws FileNotFoundException {
		requireOpenState();
		copyFileToNode(getNodeMetadata().asList().get(nodeIndex), file, remoteLocation);
	}
	
	/**
	 * Asynchronous version of {@link ServerCollection#copyFileToNode(int, String, String)}
	 * @see ServerCollection#copyFileToNode(int, String, String)
	 * @param nodeIndex node index
	 * @param content content to copy
	 * @param remoteLocation remote path of file
	 * @return Future object to access result once completed
	 */
	public Future<?> copyFileToNodeAsync(final int nodeIndex, final String content, final String remoteLocation) {
		return threadPool.submit(new Callable<Void>() {

			public Void call() {
				copyFileToNode(nodeIndex, content, remoteLocation);
				return null;
			}
			
		});
	}
	
	/**
	 * Copies a file to a node
	 * @param nodeIndex node index
	 * @param content content to copy
	 * @param remoteLocation remote path of file
	 */
	public void copyFileToNode(final int nodeIndex, final String content, final String remoteLocation) {
		requireOpenState();
		copyFileToNode(getNodeMetadata().asList().get(nodeIndex), content, remoteLocation);
	}
	
	/**
	 * Asynchronous version of {@link ServerCollection#copyFileToNode(NodeMetadata, File, String)}
	 * @see ServerCollection#copyFileToNode(NodeMetadata, File, String)
	 * @param node node to copy the file to
	 * @param file file to copy
	 * @param remoteLocation remote path of file
	 * @return Future object to access result once completed
	 */
	public Future<?> copyFileToNodeAsync(final NodeMetadata node, final File file, final String remoteLocation) {
		return threadPool.submit(new Callable<Void>() {

			public Void call() {
				copyFileToNode(node, file, remoteLocation);
				return null;
			}
			
		});
	}
	
	/**
	 * Copies a file to a node.
	 * 
	 * @param node node to copy to
	 * @param file data to copy
	 * @param remoteLocation remote path of file
	 */
	public void copyFileToNode(final NodeMetadata node, final File file, final String remoteLocation) {
		requireOpenState();
		
		try {
			fileCopyThreadPool.submit(new Callable<Void>() {

				public Void call() throws FileNotFoundException {
					
					SshClient sshClient = getSshForNode(node);
					sshClient.put(remoteLocation, getFilePayload(file));
					sshClient.disconnect();
					
					return null;
				}
				
			}).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Async version of {@link ServerCollection#copyFileToNode(NodeMetadata, String, String)}
	 * Note that this method is not restricted in the number of simultaneous file transfers.
	 * 
	 * @see ServerCollection#copyFileToNode(NodeMetadata, String, String)
	 * @param node node to copy to
	 * @param content content to copy
	 * @param remoteLocation remote path of file
	 * @return Future object to access result once completed
	 */
	public Future<?> copyFileToNodeAsync(final NodeMetadata node, final String content, final String remoteLocation) {
		return threadPool.submit(new Callable<Void>() {

			public Void call() {
				copyFileToNode(node, content, remoteLocation);
				return null;
			}
			
		});
	}
	
	/**
	 * Copy a file to a node
	 * 
	 * @param node node to copy to
	 * @param content content to copy
	 * @param remoteLocation remote path of file
	 */
	public void copyFileToNode(final NodeMetadata node, final String content, final String remoteLocation) {
		requireOpenState();
		
		try {
			fileCopyThreadPool.submit(new Callable<Void>() {

				public Void call() {
					SshClient sshClient = getSshForNode(node);
					sshClient.put(remoteLocation, content);
					sshClient.disconnect();
					return null;
				}
				
			}).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Asynchronous version of {@link ServerCollection#getFileFromNode(int, String, File)}
	 * @see ServerCollection#getFileFromNode(int, String, File)
	 * @param nodeIndex index of node to copy from
	 * @param remoteFileLocation remote file path
	 * @param localFile local file path
	 * @return Future object to access result once completed
	 */
	public Future<?> getFileFromNodeAsync(final int nodeIndex, final String remoteFileLocation, final File localFile) {
		return threadPool.submit(new Callable<Void>() {

			public Void call() {
				getFileFromNode(nodeIndex, remoteFileLocation, localFile);
				return null;
			}
			
		});
	}
	
	/**
	 * Copies a remote file to local path
	 * @param nodeIndex index of node to copy from
	 * @param remoteFileLocation remote file path
	 * @param localFile local file path
	 */
	public void getFileFromNode(final int nodeIndex, final String remoteFileLocation, final File localFile) {
		requireOpenState();
		try {
			fileCopyThreadPool.submit(new Callable<Void>() {

				public Void call() {
					NodeMetadata node = getNodeMetadata().asList().get(nodeIndex);
					SshClient ssh = getSshForNode(node);
					
					InputStream inputStream = null;
					OutputStream outputStream = null;
					try {
						inputStream = ssh.get(remoteFileLocation).openStream();
						
						// write the inputStream to a FileOutputStream
						outputStream = new FileOutputStream(localFile);
						 
						int read = 0;
						byte[] bytes = new byte[1024];
						
						while ((read = inputStream.read(bytes)) != -1) {
							outputStream.write(bytes, 0, read);
						}
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						if (outputStream != null) {
							try {
								outputStream.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
						if (inputStream != null) {
							try {
								inputStream.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
						ssh.disconnect();
					}
					return null;
				}
				
			}).get();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		} catch (ExecutionException e1) {
			e1.printStackTrace();
		}
		
		
	}
	
	/**
	 * Creates an {@link org.jclouds.io.payloads.InputStreamPayload} from a file
	 * @param file file
	 * @return an InputStreamPayload
	 * @throws FileNotFoundException
	 */
	private InputStreamPayload getFilePayload(File file) throws FileNotFoundException {
		requireOpenState();
		
		InputStreamPayload payload = new InputStreamPayload(new BufferedInputStream(new FileInputStream(file)));
		BaseMutableContentMetadata metadata = new BaseMutableContentMetadata();
		metadata.setContentLength(file.length());
		payload.setContentMetadata(metadata);
		return payload;
	}
	
	/**
	 * Gets an SSH connection to a given node
	 * @param node node to connect to
	 * @return SSH connection
	 */
	protected SshClient getSshForNode(NodeMetadata node) {
		requireOpenState();
		
		SshClient client = getComputeService().getContext().utils().sshFactory().create(HostAndPort.fromParts(node.getPublicAddresses().iterator().next(), 22), LoginCredentials.builder().privateKey(getKeyPair().getPrivateKey()).user(defaultLoginUser).build());
		try {
			client.connect();
		} catch (SshException e) {
			logger.error("Lost SSH access to nodes!");
			logger.error("!!! Giving up on job !!!");
			e.printStackTrace();
			this.close();
			throw new IllegalStateException();
		}
		return client;
	}

	/**
	 * Shuts down all VMs in the collection, deletes the key pair, and closes logs
	 */
	public void close() {
		logger.debug("VM termination requested.");
		
		logger.debug("Closing all log files...");
		for (NodeMetadata node : nodeLoggersStdOut.keySet()) {
			nodeLoggersStdOut.get(node).close();
		}
		for (NodeMetadata node : nodeLoggersStdErr.keySet()) {
			nodeLoggersStdErr.get(node).close();
		}
		logger.debug("Log files closed.");
		
		logger.debug("Terminating all instances...");
		Stack<Future<?>> workers = new Stack<Future<?>>();
		for (final Server s : this) {
			workers.push(threadPool.submit(new Runnable() {
				public void run() {
					connection.terminateServer(s);
				}
			}));
		}
		while (!workers.isEmpty()) {
			try {
				workers.pop().get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		logger.debug("Instances terminated.");
		
		connection.deleteKeyPair(getKeyPair());
		isOpen = false;
		
		logger.debug("All VMs now terminated.");
	}

	/**
	 * {@inheritDoc}
	 */
	@Deprecated
	public boolean add(Server e) {
		return serverList.add(e);
	}

	/**
	 * {@inheritDoc}
	 */
	@Deprecated
	public boolean addAll(Collection<? extends Server> c) {
		return serverList.addAll(c);
	}

	/**
	 * {@inheritDoc}
	 */
	@Deprecated
	public boolean addAll(int index, Collection<? extends Server> c) {
		return serverList.addAll(c);
	}

	/**
	 * {@inheritDoc}
	 */
	@Deprecated
	public void clear() {
		serverList.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean contains(Object o) {
		return serverList.contains(o);
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean containsAll(Collection<?> c) {
		return serverList.containsAll(c);
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean isEmpty() {
		return serverList.isEmpty();
	}

	/**
	 * {@inheritDoc}
	 */
	public Iterator<Server> iterator() {
		return serverList.iterator();
	}

	/**
	 * {@inheritDoc}
	 */
	@Deprecated
	public boolean remove(Object o) {
		return serverList.remove(o);
	}

	@Deprecated
	public boolean removeAll(Collection<?> c) {
		return serverList.removeAll(c);
	}

	/**
	 * {@inheritDoc}
	 */
	@Deprecated
	public boolean retainAll(Collection<?> c) {
		return serverList.retainAll(c);
	}

	/**
	 * {@inheritDoc}
	 */
	public int size() {
		return serverList.size();
	}

	/**
	 * {@inheritDoc}
	 */
	public Object[] toArray() {
		return serverList.toArray();
	}

	/**
	 * {@inheritDoc}
	 */
	public <T> T[] toArray(T[] a) {
		return serverList.toArray(a);
	}
	
	/**
	 * Throws an {@link java.lang.IllegalStateException} if the ServerCollection
	 * is closed. Used in all methods where all nodes are expected to be active.
	 * {@link java.lang.IllegalStateException} is a runtime exception that is
	 * generally caught in implementations of {@link ResourceProvisioningCallback}
	 * such as {@link jrigby.openstack_queue.request_templates.ThreeStageJobCallback}.
	 */
	private void requireOpenState() {
		if (!isOpen) {
			throw new IllegalStateException();
		}
	}
}
