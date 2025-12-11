package com.dtc.blobutil.config;

/**
 * Main application configuration container
 */
public class AppConfig {
    private BlobStorageConfig blobStorageConfig;
    private DatabaseConfig databaseConfig;

    public BlobStorageConfig getBlobStorageConfig() {
        return blobStorageConfig;
    }

    public void setBlobStorageConfig(BlobStorageConfig blobStorageConfig) {
        this.blobStorageConfig = blobStorageConfig;
    }

    public DatabaseConfig getDatabaseConfig() {
        return databaseConfig;
    }

    public void setDatabaseConfig(DatabaseConfig databaseConfig) {
        this.databaseConfig = databaseConfig;
    }
}


