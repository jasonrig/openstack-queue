package jrigby.openstack_queue.request_templates;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * This class provides a convenient way to safely convert json into 
 * a {@link ThreeStageJobResourceRequest}.
 * @author jason
 *
 */
public class ThreeStageJobResourceRequestJsonMessage {
	
	private final static Logger logger = LoggerFactory.getLogger(ThreeStageJobResourceRequestJsonMessage.class);
	
	private String groupName = null;
	private Integer minNodes = null;
	private Integer maxNodes = null;
	private Integer minNodeSize = null;
	private String osImageId = null;
	private String defaultLoginUser = null;
	private String logPath = null;
	private String[] securityGroups = null;
	private String[] preferredAvailabilityZones = null;
	private Long jobMaximumWalltime = null;
	private String[] payloadFiles = null;
	private String bootstrapScript = null;
	private String executeScript = null;
	private String cleanupScript = null;
	private String resultsPath = null;
	
	/**
	 * This constructor ensures that this object is never directly created.
	 * Rather, we use the magic of {@link com.google.gson.Gson} to create
	 * the object for us, which circumvents this constructor.
	 * 
	 * This object should always be created via the {@link ThreeStageJobResourceRequestJsonMessage#generateFromJson(String)}
	 * method.
	 */
	private ThreeStageJobResourceRequestJsonMessage() {
		
	}

	/**
	 * Creates a {@link ThreeStageJobResourceRequest} from a json object
	 * 
	 * @param json the json object
	 * @return the resource request
	 */
	public static ThreeStageJobResourceRequest generateFromJson(String json) {
		logger.debug("Creating resource request from json message...");
		
		ThreeStageJobResourceRequestJsonMessage msg = new Gson().fromJson(json, ThreeStageJobResourceRequestJsonMessage.class);
		
		
		ThreeStageJobResourceRequest req = null;
		try { // Catch a possible null pointer exception here as a result of invalid json.
			req = new ThreeStageJobResourceRequest(msg.groupName, msg.bootstrapScript, msg.executeScript, msg.cleanupScript, msg.resultsPath, stringToFile(msg.payloadFiles));
		} catch (NullPointerException e) {
			logger.debug("Syntactically valid json produced a null pointer exception.");
			logger.debug(e.getMessage());
			throw new JsonSyntaxException(e); // This should be handled by the QueueCommandProcessor
		}
		
		// If the field is null, then stick with the defaults
		if (msg.defaultLoginUser != null) {
			req.setDefaultLoginUser(msg.defaultLoginUser);
		}
		if (msg.jobMaximumWalltime != null) {
			req.setJobMaximumWalltime(msg.jobMaximumWalltime);
		}
		if (msg.logPath != null) {
			req.setLogPath(msg.logPath);
		}
		if (msg.maxNodes != null) {
			req.setMaxNodes(msg.maxNodes);
		}
		if (msg.minNodes != null) {
			req.setMinNodes(msg.minNodes);
		}
		if (msg.minNodeSize != null) {
			req.setMinNodeSize(msg.minNodeSize);
		}
		if (msg.osImageId != null) {
			req.setOsImageId(msg.osImageId);
		}
		if (msg.preferredAvailabilityZones != null) {
			req.setPreferredAvailabilityZones(msg.preferredAvailabilityZones);
		}
		if (msg.securityGroups != null) {
			req.setSecurityGroups(msg.securityGroups);
		}
		
		logger.debug("Resource request successfully created from json message.");
		
		return req;
	}
	
	/**
	 * Converts a string of file names into an array of java {@link java.io.File} objects
	 * @param files file names to convert
	 * @return an array of File objects
	 */
	private static File[] stringToFile(String[] files) {
		if (files == null) {
			return new File[0];
		}
		File[] f = new File[files.length];
		for (int i = 0; i < files.length; i++) {
			f[i] = new File(files[i]);
		}
		return f;
	}
}
