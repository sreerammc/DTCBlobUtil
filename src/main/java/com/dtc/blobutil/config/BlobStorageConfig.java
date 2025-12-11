package com.dtc.blobutil.config;

/**
 * Configuration for Azure Blob Storage connection
 */
public class BlobStorageConfig {
    private String accountName;
    private String containerName;
    private String connectionString;
    private boolean useManagedIdentity;
    private String tenantId;
    private String clientId;
    private String clientSecret;
    private long pollingIntervalSeconds; // Polling interval in seconds
    private boolean processHistoricalData; // If true, process all blobs on startup; if false, only new changes

    public BlobStorageConfig() {
        this.pollingIntervalSeconds = 60; // Default: poll every 60 seconds
        this.processHistoricalData = false; // Default: only process new changes after startup
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
}


