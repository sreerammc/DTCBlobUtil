package com.dtc.blobutil.config;

/**
 * Configuration for Azure Blob Storage connection
 */
public class BlobStorageConfig {
    private String accountName;
    private String containerName;
    private String archiveContainerName;
    private String connectionString;
    private boolean useManagedIdentity;
    private String tenantId;
    private String clientId;
    private String clientSecret;
    private long pollingIntervalSeconds; // Polling interval in seconds
    private boolean processHistoricalData; // If true, process all blobs on startup; if false, only new changes
    private int archiveProcessingDelayMinutes; // Minimum age in minutes before processing archive files

    public BlobStorageConfig() {
        this.pollingIntervalSeconds = 60; // Default: poll every 60 seconds
        this.processHistoricalData = false; // Default: only process new changes after startup
        this.archiveProcessingDelayMinutes = 10; // Default: process files older than 10 minutes
    }

    public String getAccountName() {
        return accountName;
    }

    public void setAccountName(String accountName) {
        this.accountName = accountName;
    }

    public String getContainerName() {
        return containerName;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public String getArchiveContainerName() {
        return archiveContainerName;
    }

    public void setArchiveContainerName(String archiveContainerName) {
        this.archiveContainerName = archiveContainerName;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public boolean isUseManagedIdentity() {
        return useManagedIdentity;
    }

    public void setUseManagedIdentity(boolean useManagedIdentity) {
        this.useManagedIdentity = useManagedIdentity;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public long getPollingIntervalSeconds() {
        return pollingIntervalSeconds;
    }

    public void setPollingIntervalSeconds(long pollingIntervalSeconds) {
        this.pollingIntervalSeconds = pollingIntervalSeconds;
    }

    public boolean isProcessHistoricalData() {
        return processHistoricalData;
    }

    public void setProcessHistoricalData(boolean processHistoricalData) {
        this.processHistoricalData = processHistoricalData;
    }

    public int getArchiveProcessingDelayMinutes() {
        return archiveProcessingDelayMinutes;
    }

    public void setArchiveProcessingDelayMinutes(int archiveProcessingDelayMinutes) {
        this.archiveProcessingDelayMinutes = archiveProcessingDelayMinutes;
    }
}


