package com.dtc.blobutil;

import com.dtc.blobutil.config.AppConfig;
import com.dtc.blobutil.config.BlobStorageConfig;
import com.dtc.blobutil.config.ConfigLoader;
import com.dtc.blobutil.config.DatabaseConfig;
import com.dtc.blobutil.config.InfluxConfig;
import com.dtc.blobutil.dao.BlobChangeDao;
import com.dtc.blobutil.influx.InfluxClient;
import com.dtc.blobutil.influx.InfluxClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Main utility class to verify archive data in InfluxDB using FlightSQL.
 *
 * For each blob that has completed archive processing (processing_status = 'COMPLETED'
 * or 'VERIFIED_FAILED'), this processor:
 *  - Builds a SQL query from the configured template, injecting the blob file name.
 *  - Executes the query against InfluxDB 3 via FlightSQL.
 *  - Updates processing_status in blob_changes to:
 *      - 'VERIFIED_OK' on success
 *      - 'VERIFIED_FAILED' on failure after retries
 */
public class InfluxVerificationProcessor {
    private static final Logger logger = LoggerFactory.getLogger(InfluxVerificationProcessor.class);
    private static final int MAX_RETRIES = 3;

    public static void main(String[] args) {
        // Parse command line arguments (same style as other mains)
        String configFilePath = null;
        if (args.length > 0) {
            if (args[0].equals("-h") || args[0].equals("--help")) {
                printUsage();
                System.exit(0);
            } else if (args[0].equals("-c") || args[0].equals("--config")) {
                if (args.length < 2) {
                    System.err.println("Error: Config file path required after -c/--config");
                    printUsage();
                    System.exit(1);
                }
                configFilePath = args[1];
            } else {
                // Assume first argument is config file path for backward compatibility
                configFilePath = args[0];
            }
        }

        logger.info("Starting Influx Verification Processor");
        if (configFilePath != null) {
            logger.info("Using config file: {}", configFilePath);
        }

        try {
            AppConfig config = ConfigLoader.loadConfig(configFilePath);
            validateConfig(config);

            // Database initialization
            DatabaseConfig dbConfig = config.getDatabaseConfig();
            logger.info("Connecting to PostgreSQL for state tracking:");
            logger.info("  Host: {}", dbConfig.getHost());
            logger.info("  Port: {}", dbConfig.getPort());
            logger.info("  Database: {}", dbConfig.getDatabase());
            logger.info("  Username: {}", dbConfig.getUsername());
            logger.info("  SSL Mode: {}", dbConfig.getSslMode());

            DataSource dataSource = BlobChangeDao.createDataSource(
                dbConfig.getJdbcUrl(),
                dbConfig.getUsername(),
                dbConfig.getPassword(),
                dbConfig.getMaxPoolSize()
            );

            BlobChangeDao dao = new BlobChangeDao(
                dataSource,
                dbConfig.getSchema(),
                dbConfig.getTableName()
            );

            // Ensure table exists and has the required columns
            dao.initializeTable();

            // Influx / FlightSQL config
            InfluxConfig influxConfig = config.getInfluxConfig();
            String protocol = influxConfig.getProtocol();
            logger.info("InfluxDB endpoint: {}:{}, protocol: '{}', database: {}",
                influxConfig.getHost(), influxConfig.getPort(), 
                protocol, influxConfig.getDatabase());
            
            // Validate protocol is set
            if (protocol == null || protocol.isEmpty()) {
                logger.warn("InfluxDB protocol is not set, defaulting to 'grpc'");
            } else {
                logger.info("Using InfluxDB protocol: '{}' (will create {} client)", 
                    protocol, 
                    ("grpc".equalsIgnoreCase(protocol) ? "FlightSQL" : "HTTP"));
            }

            if (influxConfig.getQueryTemplate() == null || influxConfig.getQueryTemplate().isEmpty()) {
                throw new IllegalArgumentException("influx.queryTemplate is required in configuration");
            }

            // Optional: reuse blob polling interval for verification loop
            BlobStorageConfig blobConfig = config.getBlobStorageConfig();
            long pollingIntervalSeconds = blobConfig != null ? blobConfig.getPollingIntervalSeconds() : 60;
            logger.info("Verification polling interval: {} seconds", pollingIntervalSeconds);

            try (InfluxClient influxClient = InfluxClientFactory.create(influxConfig)) {
                // Simple continuous loop (like other processors)
                boolean running = true;
                while (running) {
                    try {
                        processInfluxVerifications(dao, influxClient, influxConfig.getQueryTemplate());

                        logger.debug("Waiting {} seconds before next verification cycle...", pollingIntervalSeconds);
                        Thread.sleep(TimeUnit.SECONDS.toMillis(pollingIntervalSeconds));
                    } catch (InterruptedException e) {
                        logger.info("Interrupted. Shutting down Influx verification...");
                        running = false;
                    } catch (Exception e) {
                        logger.error("Error during Influx verification cycle. Will retry in {} seconds",
                            pollingIntervalSeconds, e);
                        try {
                            Thread.sleep(TimeUnit.SECONDS.toMillis(pollingIntervalSeconds));
                        } catch (InterruptedException ie) {
                            logger.info("Interrupted during error recovery. Shutting down...");
                            running = false;
                        }
                    }
                }
            }

            logger.info("Influx Verification Processor stopped");
        } catch (Exception e) {
            logger.error("Fatal error during Influx verification", e);
            System.exit(1);
        }
    }

    /**
     * For each blob that has completed archive processing, run the configured Influx query
     * and update processing_status accordingly.
     */
    private static void processInfluxVerifications(BlobChangeDao dao,
                                                   InfluxClient influxClient,
                                                   String queryTemplate) {
        try {
            List<String> blobNames = dao.getBlobNamesForInfluxVerification();

            if (blobNames.isEmpty()) {
                logger.debug("No blobs found that need Influx verification");
                return;
            }

            logger.info("Found {} blobs to verify in Influx", blobNames.size());

            int verifiedOk = 0;
            int verifiedFailed = 0;

            for (String blobName : blobNames) {
                try {
                    logger.debug("Verifying blob in Influx: {}", blobName);

                    // Mark as verifying (best-effort)
                    try {
                        dao.updateProcessingStatus(blobName, "VERIFYING");
                    } catch (Exception e) {
                        logger.warn("Failed to update status to VERIFYING for blob: {}", blobName, e);
                    }

                    String sql = String.format(queryTemplate, blobName);

                    long count = executeWithRetry(influxClient, sql, blobName);
                    logger.info("Influx count(*) for blob {} is {}", blobName, count);

                    try {
                        // Update both InfluxDB count and status
                        dao.updateInfluxCountAndStatus(blobName, count, "VERIFIED_OK");
                    } catch (Exception e) {
                        logger.error("Failed to update InfluxDB count and status to VERIFIED_OK for blob: {}", blobName, e);
                    }

                    verifiedOk++;
                } catch (Exception e) {
                    verifiedFailed++;
                    logger.error("Error verifying blob in Influx after retries: {}", blobName, e);
                    try {
                        dao.updateProcessingStatus(blobName, "VERIFIED_FAILED");
                    } catch (Exception statusException) {
                        logger.error("Failed to update status to VERIFIED_FAILED for blob: {}",
                            blobName, statusException);
                    }
                }
            }

            logger.info("Influx verification cycle complete. OK: {}, Failed: {}", verifiedOk, verifiedFailed);
        } catch (Exception e) {
            logger.error("Error running Influx verification cycle", e);
            throw new RuntimeException("Failed to run Influx verification cycle", e);
        }
    }

    /**
     * Execute a count(*) query with simple retry logic.
     */
    private static long executeWithRetry(InfluxClient influxClient, String sql, String blobName) throws Exception {
        int attempt = 0;
        Exception lastException = null;

        while (attempt <= MAX_RETRIES) {
            try {
                if (attempt > 0) {
                    logger.info("Retry attempt {} of {} for Influx verification of blob: {}",
                        attempt, MAX_RETRIES, blobName);
                    Thread.sleep((long) Math.pow(2, attempt - 1) * 1000);
                }
                return influxClient.queryCount(sql);
            } catch (Exception e) {
                lastException = e;
                attempt++;
                if (attempt <= MAX_RETRIES) {
                    logger.warn("Attempt {} failed for blob {}. Will retry...", attempt, blobName, e);
                }
            }
        }

        throw new RuntimeException("Failed to execute Influx query for blob " + blobName +
            " after " + MAX_RETRIES + " retries", lastException);
    }

    private static void validateConfig(AppConfig config) {
        if (config.getBlobStorageConfig() == null) {
            throw new IllegalArgumentException("Blob storage configuration is required");
        }

        if (config.getDatabaseConfig() == null) {
            throw new IllegalArgumentException("Database configuration is required");
        }

        if (config.getInfluxConfig() == null) {
            throw new IllegalArgumentException("Influx configuration is required");
        }

        BlobStorageConfig blobConfig = config.getBlobStorageConfig();
        if (blobConfig.getContainerName() == null || blobConfig.getContainerName().isEmpty()) {
            throw new IllegalArgumentException("Blob container name is required");
        }

        DatabaseConfig dbConfig = config.getDatabaseConfig();
        if (dbConfig.getHost() == null || dbConfig.getHost().isEmpty()) {
            throw new IllegalArgumentException("Database host is required");
        }

        if (dbConfig.getDatabase() == null || dbConfig.getDatabase().isEmpty()) {
            throw new IllegalArgumentException("Database name is required");
        }

        if (dbConfig.getUsername() == null || dbConfig.getUsername().isEmpty()) {
            throw new IllegalArgumentException("Database username is required");
        }

        if (dbConfig.getPassword() == null || dbConfig.getPassword().isEmpty()) {
            throw new IllegalArgumentException("Database password is required");
        }

        InfluxConfig influxConfig = config.getInfluxConfig();
        if (influxConfig.getHost() == null || influxConfig.getHost().isEmpty()) {
            throw new IllegalArgumentException("Influx host is required");
        }
        if (influxConfig.getPort() <= 0) {
            throw new IllegalArgumentException("Influx port must be > 0");
        }
        if (influxConfig.getDatabase() == null || influxConfig.getDatabase().isEmpty()) {
            throw new IllegalArgumentException("Influx database is required");
        }
        if (influxConfig.getToken() == null || influxConfig.getToken().isEmpty()) {
            throw new IllegalArgumentException("Influx token is required");
        }
    }

    private static void printUsage() {
        System.out.println("Influx Verification Processor");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -cp blob-util-1.0.0.jar com.dtc.blobutil.InfluxVerificationProcessor [options] [config-file]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -c, --config <file>    Path to configuration file");
        System.out.println("  -h, --help             Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -cp blob-util-1.0.0.jar com.dtc.blobutil.InfluxVerificationProcessor");
        System.out.println("  java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -cp blob-util-1.0.0.jar com.dtc.blobutil.InfluxVerificationProcessor my-config.conf");
        System.out.println("  java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -cp blob-util-1.0.0.jar com.dtc.blobutil.InfluxVerificationProcessor -c /path/to/config.conf");
        System.out.println();
        System.out.println("Note: The --add-opens JVM argument is required for Java 9+ when using Apache Arrow");
        System.out.println("      (used by InfluxDB 3 FlightSQL client). This exposes JDK internals needed by Arrow.");
        System.out.println();
        System.out.println("This utility verifies archived blob data in InfluxDB 3 using FlightSQL");
        System.out.println("by running a configured count(*) query per blob file name and updating");
        System.out.println("the processing_status field in the blob_changes table.");
    }
}



