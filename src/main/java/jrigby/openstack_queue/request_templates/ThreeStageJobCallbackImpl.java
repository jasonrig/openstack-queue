package jrigby.openstack_queue.request_templates;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Stack;
import java.util.UUID;

import jrigby.openstack_queue.server.ResourcesTemporarilyExceededException;
import jrigby.openstack_queue.server.ServerCollection;

import jrigby.openstack_queue.server.ServerSettings;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.utils.IOUtils;
import org.jclouds.compute.RunScriptOnNodesException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A full implementation of {@link ThreeStageJobCallback}
 * where the three stages are supplied as String arguments
 * for remote execution.
 * 
 * @author jason
 *
 */
public class ThreeStageJobCallbackImpl extends ThreeStageJobCallback {
	
	private final static Logger logger = LoggerFactory.getLogger(ThreeStageJobCallbackImpl.class);
	
	private File[] payloadFiles;
	
	private String bootstrapScript, executeScript, cleanupScript, resultsPath;
	
	/**
	 * @param bootstrapScript the bootstrap script to execute
	 * @param executeScript the main execution script to execute
	 * @param cleanupScript the cleanup script to execute (run before results files are returned)
	 * @param resultsPath path of the results files
	 * @param payloadFiles path of the payload files to copy to the remote hosts
	 */
	public ThreeStageJobCallbackImpl(String bootstrapScript, String executeScript, String cleanupScript, String resultsPath, File...payloadFiles) {
		this.bootstrapScript = bootstrapScript;
		this.executeScript = executeScript;
		this.cleanupScript = cleanupScript;
		this.resultsPath = resultsPath;
		this.payloadFiles = payloadFiles;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean onFailure(Throwable e) {
		try {
			throw e;
		} catch (ResourcesTemporarilyExceededException e1) {
			logger.debug("Resources temporarily exceeded. Rescheduling job.");
			return true;
		} catch (Throwable e1) {
            if (ServerSettings.getInstance().rescheduleIfResourcesExhausted()) {
                logger.debug("Could not create nodes this time. Rescheduling job.");
                return true;
            } else {
                logger.debug("Could not create nodes, not rescheduling.");
                return false;
            }
		}
	}

	/**
	 * The bootstrap phase initially copies all payload files
	 * to the remote host. Then the custom script is executed.
	 */
	public void bootstrap(final ServerCollection nodes) {
		// Distribute payload files
		logger.debug("Distributing payload to all nodes...");
		// Put payload files into a zip archive
		File payloadTarFile = new File(UUID.randomUUID().toString()+".tar");
        payloadTarFile.deleteOnExit();
		try {
			FileOutputStream payloadTarOutputStream = new FileOutputStream(payloadTarFile);
			ArchiveOutputStream payloadTarball = new ArchiveStreamFactory().createArchiveOutputStream(ArchiveStreamFactory.TAR, payloadTarOutputStream);
			
			for (File payloadFile : payloadFiles) {
				TarArchiveEntry payloadTarEntry = new TarArchiveEntry(payloadFile.getName());
				payloadTarEntry.setSize(payloadFile.length());
				payloadTarball.putArchiveEntry(payloadTarEntry);
				IOUtils.copy(new FileInputStream(payloadFile), payloadTarball);
				payloadTarball.closeArchiveEntry();
			}
			
			payloadTarball.finish();
			payloadTarOutputStream.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ArchiveException e) {
			e.printStackTrace();
		}
		
		try {
			nodes.copyFileToAllNodes(payloadTarFile, "/tmp/"+payloadTarFile.getName());
			nodes.runScriptOnAllNodes("cd /tmp &&"
					+ "tar -xvf "+payloadTarFile.getName()+" &&"
							+ "chmod 777 ./* &&"
							+ "rm "+payloadTarFile.getName());	// Set very relaxed permissions here (saves trouble, and high security not needed here)
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (RunScriptOnNodesException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
            payloadTarFile.delete(); // Ensure that they payload file will be deleted even on a runtime exception
            throw e;
        }
		
		payloadTarFile.delete();
		logger.debug("Payload distribution complete.");
		
		// Start bootstrap script
		logger.debug("Executing custom bootstrap script on all nodes...");
		try {
			// Files in /tmp/copyback will be returned after job completion
			nodes.runScriptOnAllNodes("mkdir /tmp/copyback");
			if (bootstrapScript != null && !bootstrapScript.isEmpty()) {
				nodes.runScriptOnAllNodes(bootstrapScript);
			} else {
				logger.warn("No custom bootstrap script specified.");
			}
		} catch (RunScriptOnNodesException e) {
			logger.warn("Error executing script!");
			e.printStackTrace();
		}
		logger.debug("Custom bootstrap script execution complete.");
	}

	/**
	 * The execute script is run
	 */
	public void execute(ServerCollection nodes) {
		logger.debug("Executing custom execute script on head node only...");
		try {
			if (executeScript != null && !executeScript.isEmpty()) {
				nodes.runScriptOnNode(0, executeScript);
			} else {
				logger.warn("No execute script specified!");
			}
		} catch (RunScriptOnNodesException e) {
			logger.warn("Error executing script!");
			e.printStackTrace();
		}
		logger.debug("Execute script execution complete.");
	}

	/**
	 * The custom cleanup script is first executed. Then, any
	 * results files are compressed and returned.
	 */
	public void cleanup(final ServerCollection nodes) {
		try {
			logger.debug("Executing custom cleanup script on all nodes...");
			if (cleanupScript != null && !cleanupScript.isEmpty()) {
				nodes.runScriptOnAllNodes(cleanupScript);
			} else {
				logger.warn("No custom cleanup script specified!");
			}
			logger.debug("Custom cleanup script execution complete.");
			
			// Compress all results files on each node
			logger.debug("Compressing results before copying back...");
			nodes.runScriptOnAllNodes("cd /tmp/copyback && tar -zhcvf /tmp/copyback.tar.gz *");
			logger.debug("Compression complete.");
			Stack<Thread> workers = new Stack<Thread>();
			
			// Copy results files ("/tmp/copyback.tar.gz") back
			logger.debug("Copying back compressed files...");
			for (int i = 0; i < nodes.size(); i++) {
				final int nodeIndex = i;
				workers.push(new Thread() {
					public void run() {
						nodes.getFileFromNode(nodeIndex, "/tmp/copyback.tar.gz", new File(resultsPath+"/"+nodeIndex+"-"+nodes.hostNameOf(nodeIndex)+".results.tar.gz"));
					}
				});
				workers.peek().start();
			}
			while (!workers.isEmpty()) {
				try {
					workers.pop().join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			logger.debug("Copy complete.");
		} catch (RunScriptOnNodesException e) {
			logger.warn("Error cleaning up!");
			e.printStackTrace();
		}
		
	}

}
