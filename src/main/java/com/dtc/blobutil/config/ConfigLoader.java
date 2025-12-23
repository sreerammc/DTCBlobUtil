package com.dtc.blobutil.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Loads configuration from application.conf or environment variables
 */
public class ConfigLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);
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
        // Load default config from classpath first (for defaults)
        Config defaultConfig = ConfigFactory.load();
        
        // Try to load from file if specified or default location
        String configFileToUse = configFilePath != null ? configFilePath : DEFAULT_CONFIG_FILE;
        File configFile = findConfigFile(configFileToUse);
        
        Config config;
        if (configFile != null && configFile.exists()) {
            logger.info("Loading config from: {}", configFile.getAbsolutePath());
            // Parse the specified file - this should be the primary config
            Config fileConfig = ConfigFactory.parseFile(configFile);
            
            // Debug: Check what's in the file BEFORE merging
            if (fileConfig.hasPath("influx.protocol")) {
                String fileProtocol = fileConfig.getString("influx.protocol");
                logger.info("Config file contains influx.protocol = '{}'", fileProtocol);
            } else {
                logger.warn("Config file does NOT contain influx.protocol");
            }
            
            // Merge: file config takes precedence, default config is fallback
            // withFallback means: use fileConfig first, fall back to defaultConfig if key missing
            config = fileConfig.withFallback(defaultConfig);
            
            // Check final merged value AFTER merging
            if (config.hasPath("influx.protocol")) {
                String finalProtocol = config.getString("influx.protocol");
                logger.info("Final merged config has influx.protocol = '{}' (file should take precedence)", finalProtocol);
                
                // Verify the file value is being used
                if (fileConfig.hasPath("influx.protocol")) {
                    String fileProtocol = fileConfig.getString("influx.protocol");
                    if (!fileProtocol.equals(finalProtocol)) {
                        logger.error("WARNING: Config file protocol '{}' was overridden by default config '{}'!", 
                            fileProtocol, finalProtocol);
                    }
                }
            }
        } else if (configFilePath != null) {
            // If user specified a file path but it doesn't exist, warn them
            logger.warn("Config file not found: {}", configFilePath);
            logger.warn("Searched in: {}", new File(configFileToUse).getAbsolutePath());
            logger.warn("Falling back to environment variables and default config.");
            config = defaultConfig;
        } else {
            // No file specified, use defaults
            logger.info("No config file specified, using default config and environment variables");
            config = defaultConfig;
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

        if (config.hasPath("blob.archiveContainerName")) {
            blobConfig.setArchiveContainerName(config.getString("blob.archiveContainerName"));
        } else {
            blobConfig.setArchiveContainerName(System.getenv("BLOB_ARCHIVE_CONTAINER_NAME"));
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

        if (config.hasPath("blob.archiveProcessingDelayMinutes")) {
            blobConfig.setArchiveProcessingDelayMinutes(config.getInt("blob.archiveProcessingDelayMinutes"));
        } else if (System.getenv("BLOB_ARCHIVE_PROCESSING_DELAY_MINUTES") != null) {
            blobConfig.setArchiveProcessingDelayMinutes(Integer.parseInt(System.getenv("BLOB_ARCHIVE_PROCESSING_DELAY_MINUTES")));
        }

        appConfig.setBlobStorageConfig(blobConfig);

        // Load InfluxDB / FlightSQL config
        InfluxConfig influxConfig = new InfluxConfig();
        if (config.hasPath("influx.host")) {
            influxConfig.setHost(config.getString("influx.host"));
        } else {
            influxConfig.setHost(System.getenv("INFLUX_HOST"));
        }

        if (config.hasPath("influx.port")) {
            influxConfig.setPort(config.getInt("influx.port"));
        } else if (System.getenv("INFLUX_PORT") != null) {
            influxConfig.setPort(Integer.parseInt(System.getenv("INFLUX_PORT")));
        }

        if (config.hasPath("influx.database")) {
            influxConfig.setDatabase(config.getString("influx.database"));
        } else {
            influxConfig.setDatabase(System.getenv("INFLUX_DATABASE"));
        }

        if (config.hasPath("influx.token")) {
            influxConfig.setToken(config.getString("influx.token"));
        } else {
            influxConfig.setToken(System.getenv("INFLUX_TOKEN"));
        }

        if (config.hasPath("influx.queryTemplate")) {
            influxConfig.setQueryTemplate(config.getString("influx.queryTemplate"));
        } else {
            influxConfig.setQueryTemplate(System.getenv("INFLUX_QUERY_TEMPLATE"));
        }

        if (config.hasPath("influx.skipTlsValidation")) {
            influxConfig.setSkipTlsValidation(config.getBoolean("influx.skipTlsValidation"));
        } else if (System.getenv("INFLUX_SKIP_TLS_VALIDATION") != null) {
            influxConfig.setSkipTlsValidation(Boolean.parseBoolean(System.getenv("INFLUX_SKIP_TLS_VALIDATION")));
        }

        if (config.hasPath("influx.protocol")) {
            String protocolValue = config.getString("influx.protocol");
            logger.info("Found influx.protocol in config: '{}'", protocolValue);
            influxConfig.setProtocol(protocolValue);
            logger.debug("Set protocol to: '{}'", influxConfig.getProtocol());
        } else if (System.getenv("INFLUX_PROTOCOL") != null) {
            String envProtocol = System.getenv("INFLUX_PROTOCOL");
            logger.info("Found INFLUX_PROTOCOL env var: '{}'", envProtocol);
            influxConfig.setProtocol(envProtocol);
        } else if (config.hasPath("influx.useHttps")) {
            // Backward compatibility: useHttps option
            logger.warn("Using deprecated influx.useHttps option (use influx.protocol instead)");
            influxConfig.setUseHttps(config.getBoolean("influx.useHttps"));
        } else if (System.getenv("INFLUX_USE_HTTPS") != null) {
            logger.warn("Using deprecated INFLUX_USE_HTTPS env var (use INFLUX_PROTOCOL instead)");
            influxConfig.setUseHttps(Boolean.parseBoolean(System.getenv("INFLUX_USE_HTTPS")));
        } else {
            logger.info("No protocol specified, using default: 'grpc'");
        }
        
        logger.info("Final InfluxDB protocol value: '{}'", influxConfig.getProtocol());

        appConfig.setInfluxConfig(influxConfig);

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


