package jrigby.openstack_queue.server;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jrigby.openstack_queue.request_templates.ThreeStageJobCallback;
import jrigby.openstack_queue.request_templates.ThreeStageJobResourceRequestJsonMessage;

import com.google.gson.JsonSyntaxException;

/**
 * Processes any command received that alters the queue state
 * e.g. by submitting jobs, deleting jobs or shutting down the queue.
 * 
 * @author jason
 *
 */
public class QueueCommandProcessor {
	
	private final static Logger logger = LoggerFactory.getLogger(ThreeStageJobCallback.class);
	
	private ResourceQueue queue;
	
	/**
	 * Command type enumeration
	 * @author jason
	 *
	 */
	public enum CommandType {
		JOB_SUBMIT,
		ADMIN;
	}
	
	/**
	 * Constructor must take a {@link ResourceQueue} on which
	 * to operate.
	 * 
	 * @param queue the queue affected by received commands
	 */
	public QueueCommandProcessor(ResourceQueue queue) {
		this.queue = queue;
	}
	
	/**
	 * Processes a queue command
	 * @param command the command
	 * @param type the command type
	 */
	public void processCommand(String command, CommandType type) {
		switch (type) {
		case JOB_SUBMIT:
			logger.debug("Job submit command received, processing...");
			jobSubmitCommand(command);
			break;
		case ADMIN:
			logger.debug("Admin command received, processing...");
			adminCommand(command);
			break;
		}
	}
	
	/**
	 * Job submit commands are purely json objects. In this implementation, json is
	 * interpreted as a {@link jrigby.openstack_queue.request_templates.ThreeStageJobResourceRequest},
	 * which expects a bootstrap, execute and cleanup script. The json message itself is processed
	 * by {@link jrigby.openstack_queue.request_templates.ThreeStageJobResourceRequestJsonMessage}.
	 * Invalid queue requests are just ignored.
	 * 
	 * @param command json message
	 */
	// TODO: work out a way to inform the user of an invalid submit command -- probably best placed in the client software
	private void jobSubmitCommand(String command) {
		try {
			ResourceRequest req = ThreeStageJobResourceRequestJsonMessage.generateFromJson(command);
			logger.debug("Adding job to the queue.");
			queue.add(req);
		} catch (JsonSyntaxException e) {
			logger.debug("Syntax error detected in the json message. Ignoring the request.");
		}
	}
	
	/**
	 * Admin commands consist of a keyword followed by values that may be grouped with quotation marks
	 * if required. The only admin command that is not handled here is the "shutdown" command.
	 * @param command
	 */
	private void adminCommand(String command) {
		// shutdown already handled by this point...
		
		// Ignore any empty messages
		if (command.trim().isEmpty()) {
			logger.debug("Empty command received. Ignoring.");
			return;
		}
		// Commands are case insensitive; change everything to lower case
		command=command.toLowerCase();
		
		// Split the command up into space-separated components
		String[] commandParts = splitCommand(command);
		
		try {
			// ### "killjob" command ###
			// Deletes a job from the queue
			if (commandParts[0].equals("killjob")) {
				logger.debug("killjob command received");
				// process killjob command
				if (commandParts.length != 2) {
					logger.debug("Invalid number of parameters specified. Ignoring.");
					throw new QueueCommandException();
				}
				int jobId = Integer.valueOf(commandParts[1]);
				queue.killJob(jobId);
			
			// ### "reloadconfig" command ###
			// Reloads the configuration file
			} else if (commandParts[0].equals("reloadconfig")) {
				ServerSettings.reloadConfigurationFile();
				logger.debug("Configuration file relaoded.");
			}
		} catch (QueueCommandException e) {
			
		} catch (NumberFormatException e) {
			logger.debug("Number format exception encountered! Ignoring.");
			e.printStackTrace();
		}
	}
	
	/**
	 * Splits the text into space separated elements except where enclosed
	 * in quotation marks.
	 * 
	 * @param command the command parameters to split
	 * @return a string array of commands
	 */
	private String[] splitCommand(String command) {
		List<String> commandPartsList = new LinkedList<String>();
		Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(command);
		while (m.find()) {
		    commandPartsList.add(m.group(1).replace("\"", ""));
		}
		
		String[] commandParts = new String[commandPartsList.size()];
		return commandPartsList.toArray(commandParts);
	}

}
