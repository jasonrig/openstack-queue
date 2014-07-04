package jrigby.openstack_queue.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.jclouds.compute.domain.NodeMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains a list of authorised SSH keys in the user's home directory.
 * Assumes a linux distribution that uses ~/.ssh/authorized_keys.
 * This is a singleton object to ensure that only one set of SSH keys is ever
 * maintained.
 * 
 * @author jason
 *
 */
public class AuthorizedKeysList {
	
	private final static Logger logger = LoggerFactory.getLogger(AuthorizedKeysList.class);
	
	private static AuthorizedKeysList obj;
	
	private final String USER_HOME = System.getProperty("user.home");
	private final File AUTH_KEY_FILE = new File(USER_HOME+"/.ssh/authorized_keys");
	private final Map<NodeMetadata, String> keys = new HashMap<NodeMetadata, String>();
	
	/**
	 * Private constructor. This class may not be instantiated directly.
	 */
	private AuthorizedKeysList() {
		
	}
	
	/**
	 * @return an instance of the AuthorizedKeysList
	 */
	public static AuthorizedKeysList getInstance() {
		if (obj == null) {
			obj = new AuthorizedKeysList();
		}
		
		return obj;
	}
	
	/**
	 * Registers a public key
	 * @param node the node to which this key belongs
	 * @param publicKey the public key to register
	 */
	public void registerPublicKey(NodeMetadata node, String publicKey) {
		keys.put(node, publicKey);
		regenerateAuthorizedKeysFile();
		logger.debug("Registered public key for "+node.getHostname());
	}
	
	/**
	 * Deregisters a public key
	 * @param node the node to deregister
	 */
	public void deregisterPublicKey(NodeMetadata node) {
		keys.remove(node);
		regenerateAuthorizedKeysFile();
		logger.debug("Deregistered public key for "+node.getHostname());
	}
	
	/**
	 * Generates the public key file list
	 */
	private void regenerateAuthorizedKeysFile() {
		if (AUTH_KEY_FILE.exists()) {
			AUTH_KEY_FILE.delete();
		}
		
		try {
			PrintWriter out = new PrintWriter(AUTH_KEY_FILE);
			for (NodeMetadata node : keys.keySet()) {
				String publicKeys = keys.get(node);
				out.println(publicKeys);
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
			logger.warn("Could not write to "+AUTH_KEY_FILE.getAbsolutePath());
		}
	}
	
}
