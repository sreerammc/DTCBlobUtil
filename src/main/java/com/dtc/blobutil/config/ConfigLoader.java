package com.dtc.blobutil.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

/**
 * Loads configuration from application.conf or environment variables
 */
public class ConfigLoader {
    private static final String DEFAULT_CONFIG_FILE = "application.conf";

    /**
     * Load configuration from default location or environment variables
     */
    public static AppConfig loadConfig() {
        return loadConfig(null);
    }

    /**
     * Load configuration from specified file path or environment variables
     * @param configFilePath Path to the configuration file (null to use default)
     */
    public static AppConfig loadConfig(String configFilePath) {
        Config config = ConfigFactory.load();
        
        // Try to load from file if specified or default location
        String configFileToUse = configFilePath != null ? configFilePath : DEFAULT_CONFIG_FILE;
        File configFile = findConfigFile(configFileToUse);
        
        if (configFile != null && configFile.exists()) {
            config = ConfigFactory.parseFile(configFile).withFallback(config);
        } else if (configFilePath != null) {
            // If user specified a file path but it doesn't exist, warn them
            System.err.println("Warning: Config file not found: " + configFilePath);
            System.err.println("Searched in: " + new File(configFileToUse).getAbsolutePath());
            System.err.println("Falling back to environment variables and default config.");
        }

        AppConfig appConfig = new AppConfig();

        // Load Blob Storage config
        BlobStorageConfig blobConfig = new BlobStorageConfig();
        if (config.hasPath("blob.accountName")) {
            blobConfig.setAccountName(config.getString("blob.accountName"));
        } else {
            blobConfig.setAccountName(System.getenv("BLOB_ACCOUNT_NAME"));
        }

        if (config.hasPath("blob.containerName")) {
            blobConfig.setContainerName(config.getString("blob.containerName"));
        } else {
            blobConfig.setContainerName(System.getenv("BLOB_CONTAINER_NAME"));
        }

        if (config.hasPath("blob.connectionString")) {
            blobConfig.setConnectionString(config.getString("blob.connectionString"));
        } else {
            blobConfig.setConnectionString(System.getenv("BLOB_CONNECTION_STRING"));
        }

        if (config.hasPath("blob.useManagedIdentity")) {
            blobConfig.setUseManagedIdentity(config.getBoolean("blob.useManagedIdentity"));
        } else {
            blobConfig.setUseManagedIdentity(Boolean.parseBoolean(System.getenv("BLOB_USE_MANAGED_IDENTITY")));
        }

        if (config.hasPath("blob.tenantId")) {
            blobConfig.setTenantId(config.getString("blob.tenantId"));
        } else {
            blobConfig.setTenantId(System.getenv("BLOB_TENANT_ID"));
        }

        if (config.hasPath("blob.clientId")) {
            blobConfig.setClientId(config.getString("blob.clientId"));
        } else {
            blobConfig.setClientId(System.getenv("BLOB_CLIENT_ID"));
        }

        if (config.hasPath("blob.clientSecret")) {
            blobConfig.setClientSecret(config.getString("blob.clientSecret"));
        } else {
            blobConfig.setClientSecret(System.getenv("BLOB_CLIENT_SECRET"));
        }

        if (config.hasPath("blob.pollingIntervalSeconds")) {
            blobConfig.setPollingIntervalSeconds(config.getLong("blob.pollingIntervalSeconds"));
        } else if (System.getenv("BLOB_POLLING_INTERVAL_SECONDS") != null) {
            blobConfig.setPollingIntervalSeconds(Long.parseLong(System.getenv("BLOB_POLLING_INTERVAL_SECONDS")));
        }

        if (config.hasPath("blob.processHistoricalData")) {
            blobConfig.setProcessHistoricalData(config.getBoolean("blob.processHistoricalData"));
        } else if (System.getenv("BLOB_PROCESS_HISTORICAL_DATA") != null) {
            blobConfig.setProcessHistoricalData(Boolean.parseBoolean(System.getenv("BLOB_PROCESS_HISTORICAL_DATA")));
        }

        appConfig.setBlobStorageConfig(blobConfig);

        // Load Database config
        DatabaseConfig dbConfig = new DatabaseConfig();
        if (config.hasPath("database.host")) {
            dbConfig.setHost(config.getString("database.host"));
        } else {
            dbConfig.setHost(System.getenv("DB_HOST"));
        }

        if (config.hasPath("database.port")) {
            dbConfig.setPort(config.getInt("database.port"));
        } else if (System.getenv("DB_PORT") != null) {
            dbConfig.setPort(Integer.parseInt(System.getenv("DB_PORT")));
        }

        if (config.hasPath("database.database")) {
            dbConfig.setDatabase(config.getString("database.database"));
        } else {
            dbConfig.setDatabase(System.getenv("DB_DATABASE"));
        }

        if (config.hasPath("database.username")) {
            dbConfig.setUsername(config.getString("database.username"));
        } else {
            dbConfig.setUsername(System.getenv("DB_USERNAME"));
        }

        if (config.hasPath("database.password")) {
            dbConfig.setPassword(config.getString("database.password"));
        } else {
            dbConfig.setPassword(System.getenv("DB_PASSWORD"));
        }

        if (config.hasPath("database.schema")) {
            dbConfig.setSchema(config.getString("database.schema"));
        } else if (System.getenv("DB_SCHEMA") != null) {
            dbConfig.setSchema(System.getenv("DB_SCHEMA"));
        }

        if (config.hasPath("database.tableName")) {
            dbConfig.setTableName(config.getString("database.tableName"));
        } else if (System.getenv("DB_TABLE_NAME") != null) {
            dbConfig.setTableName(System.getenv("DB_TABLE_NAME"));
        }

        if (config.hasPath("database.maxPoolSize")) {
            dbConfig.setMaxPoolSize(config.getInt("database.maxPoolSize"));
        } else if (System.getenv("DB_MAX_POOL_SIZE") != null) {
            dbConfig.setMaxPoolSize(Integer.parseInt(System.getenv("DB_MAX_POOL_SIZE")));
        }

        if (config.hasPath("database.ssl")) {
            dbConfig.setSsl(config.getBoolean("database.ssl"));
        } else if (System.getenv("DB_SSL") != null) {
            dbConfig.setSsl(Boolean.parseBoolean(System.getenv("DB_SSL")));
        }

        if (config.hasPath("database.sslMode")) {
            dbConfig.setSslMode(config.getString("database.sslMode"));
        } else if (System.getenv("DB_SSL_MODE") != null) {
            dbConfig.setSslMode(System.getenv("DB_SSL_MODE"));
        }

        appConfig.setDatabaseConfig(dbConfig);

        return appConfig;
    }

    /**
     * Find config file in common locations
     * @param configFilePath The config file path to search for
     * @return File object if found, null otherwise
     */
    private static File findConfigFile(String configFilePath) {
        File configFile = new File(configFilePath);
        
        // If absolute path or exists in current directory, return it
        if (configFile.isAbsolute() || configFile.exists()) {
            return configFile;
        }
        
        // Try common locations
        String[] searchPaths = {
            configFilePath,  // Current directory (already checked, but keep for absolute paths)
            "src/main/resources/" + configFilePath,  // Maven resources directory
            "config/" + configFilePath,  // Config subdirectory
            "../" + configFilePath  // Parent directory
        };
        
        for (String path : searchPaths) {
            File candidate = new File(path);
            if (candidate.exists() && candidate.isFile()) {
                return candidate;
            }
        }
        
        return configFile;  // Return original path even if not found (for error message)
    }
}


