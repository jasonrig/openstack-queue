package jrigby.openstack_queue.server;

/**
 * Defines a submitted job's status in the queue
 * 
 * @author jason
 *
 */
public class JobStatus implements Comparable<JobStatus> {
	
	private Integer id;
	private String name;
	private Long startTime;
	private Long timeLimit;
	private String status;
	
	/**
	 * Get a numeric identifier for this job
	 * @return job ID
	 */
	public Integer getId() {
		return id;
	}
	
	/**
	 * Sets the job ID
	 * @param id
	 */
	public void setId(Integer id) {
		this.id = id;
	}
	
	/**
	 * Gets the name of the job
	 * @return name of job
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * Sets the name of the job
	 * @param name
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * Gets the start time of this job
	 * @return start time of job in milliseconds
	 */
	public Long getStartTime() {
		return startTime;
	}
	
	/**
	 * Sets the start time of this job
	 * @param startTime job start time in milliseconds
	 */
	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}
	
	/**
	 * Gets the time limit of this job. Setting to zero results in
	 * an unlimited run time.
	 * @return time limit in milliseconds
	 */
	public Long getTimeLimit() {
		return timeLimit;
	}
	
	/**
	 * Set the time limit for this job. Setting to zero results in
	 * an unlimited run time.
	 * @param timeLimit job time limit in milliseconds
	 */
	public void setTimeLimit(Long timeLimit) {
		this.timeLimit = timeLimit;
	}
	
	/**
	 * Gets the status of this job (e.g. WAITING, RUNNING, etc.)
	 * @return current status
	 */
	public String getStatus() {
		return status;
	}
	
	/**
	 * Sets the current status of this job
	 * @param status
	 */
	public void setStatus(String status) {
		this.status = status;
	}
	
	/**
	 * Checks if the job is running overtime
	 * @return true if time has been exceeded
	 */
	public boolean timeLimitExceeded() {
		if (timeLimit > 0) {
			return System.currentTimeMillis() > (startTime + timeLimit);
		} else {
			return false;
		}
	}
	
	/**
	 * Comparator that is based on job ID. Used for
	 * collections implementing {@link java.util.SortedSet} or
	 * are otherwise sorted.
	 */
	public int compareTo(JobStatus arg0) {
		return id.compareTo(arg0.getId());
	}
}
