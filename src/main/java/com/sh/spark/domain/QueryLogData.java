package com.sh.spark.domain;

import java.io.Serializable;

public class QueryLogData implements Serializable{

	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7244049852007343992L;
	
	private String userName;
	private String appID;
	private String clientID;
	private String startTime;
	private double ampCpuTime;
	private double totalIOCount;
	private double parserCPUTime;
	private String firstRespTime;
	private String firstStepTime;
	private String procID;
	private String queryID;
	private double maxAMPCPUT;
	private double maxAmpIO;
	private double totalCPU;
	private double totalIO;
	private double queryExecutionTime;
	private String database;
	private String queryText;

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getAppID() {
		return appID;
	}

	public void setAppID(String appID) {
		this.appID = appID;
	}

	public String getClientID() {
		return clientID;
	}

	public void setClientID(String clientID) {
		this.clientID = clientID;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public double getAmpCpuTime() {
		return ampCpuTime;
	}

	public void setAmpCpuTime(double ampCpuTime) {
		this.ampCpuTime = ampCpuTime;
	}

	public double getTotalIOCount() {
		return totalIOCount;
	}

	public void setTotalIOCount(double totalIOCount) {
		this.totalIOCount = totalIOCount;
	}

	public double getParserCPUTime() {
		return parserCPUTime;
	}

	public void setParserCPUTime(double parserCPUTime) {
		this.parserCPUTime = parserCPUTime;
	}

	public String getFirstRespTime() {
		return firstRespTime;
	}

	public void setFirstRespTime(String firstRespTime) {
		this.firstRespTime = firstRespTime;
	}

	public String getFirstStepTime() {
		return firstStepTime;
	}

	public void setFirstStepTime(String firstStepTime) {
		this.firstStepTime = firstStepTime;
	}

	public String getProcID() {
		return procID;
	}

	public void setProcID(String procID) {
		this.procID = procID;
	}

	public String getQueryID() {
		return queryID;
	}

	public void setQueryID(String queryID) {
		this.queryID = queryID;
	}

	public double getMaxAMPCPUT() {
		return maxAMPCPUT;
	}

	public void setMaxAMPCPUT(double maxAMPCPUT) {
		this.maxAMPCPUT = maxAMPCPUT;
	}

	public double getMaxAmpIO() {
		return maxAmpIO;
	}

	public void setMaxAmpIO(double maxAmpIO) {
		this.maxAmpIO = maxAmpIO;
	}

	public double getTotalCPU() {
		return totalCPU;
	}

	public void setTotalCPU(double totalCPU) {
		this.totalCPU = totalCPU;
	}

	public double getTotalIO() {
		return totalIO;
	}

	public void setTotalIO(double totalIO) {
		this.totalIO = totalIO;
	}

	public double getQueryExecutionTime() {
		return queryExecutionTime;
	}

	public void setQueryExecutionTime(double queryExecutionTime) {
		this.queryExecutionTime = queryExecutionTime;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getQueryText() {
		return queryText;
	}

	public void setQueryText(String queryText) {
		this.queryText = queryText;
	}

	@Override
	public String toString() {
		return "QueryLogData [userName=" + userName + ", appID=" + appID + ", clientID=" + clientID + ", startTime="
				+ startTime + ", ampCpuTime=" + ampCpuTime + ", totalIOCount=" + totalIOCount + ", parserCPUTime="
				+ parserCPUTime + ", firstRespTime=" + firstRespTime + ", firstStepTime=" + firstStepTime + ", procID="
				+ procID + ", queryID=" + queryID + ", maxAMPCPUT=" + maxAMPCPUT + ", maxAmpIO=" + maxAmpIO
				+ ", totalCPU=" + totalCPU + ", totalIO=" + totalIO + ", queryExecutionTime=" + queryExecutionTime
				+ ", database=" + database + ", queryText=" + queryText + "]";
	}

	
}
