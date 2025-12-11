package com.dtc.blobutil;

import com.dtc.blobutil.config.AppConfig;
import com.dtc.blobutil.config.BlobStorageConfig;
import com.dtc.blobutil.config.ConfigLoader;
import com.dtc.blobutil.config.DatabaseConfig;
import com.dtc.blobutil.dao.BlobChangeDao;
import com.dtc.blobutil.model.BlobChangeEvent;
import com.dtc.blobutil.processor.BlobChangeFeedProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Main utility class to sync Azure Blob Storage change feed to PostgreSQL
 */
public class BlobChangeFeedSync {
    private static final Logger logger = LoggerFactory.getLogger(BlobChangeFeedSync.class);

    public static void main(String[] args) {
        // Parse command line arguments
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

        logger.info("Starting Blob Change Feed Sync Utility");
        if (configFilePath != null) {
            logger.info("Using config file: {}", configFilePath);
        }

        try {
            // Load configuration
            AppConfig config = ConfigLoader.loadConfig(configFilePath);
            validateConfig(config);

            // Initialize database
            DatabaseConfig dbConfig = config.getDatabaseConfig();
            logger.info("Connecting to PostgreSQL:");
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

            // Initialize table
            dao.initializeTable();

            // Initialize blob processor
            BlobChangeFeedProcessor processor = new BlobChangeFeedProcessor(
                config.getBlobStorageConfig()
            );

            // Get polling interval and historical data processing option
            long pollingInterval = config.getBlobStorageConfig().getPollingIntervalSeconds();
            boolean processHistorical = config.getBlobStorageConfig().isProcessHistoricalData();
            
            logger.info("Polling interval: {} seconds", pollingInterval);
            if (processHistorical) {
                logger.info("Mode: Will process ALL blobs (historical + new) on startup, then only new changes");
            } else {
                logger.info("Mode: Will process ONLY NEW changes after startup (default)");
            }
            logger.info("Starting continuous monitoring mode. Press Ctrl+C to stop.");

            // Add shutdown hook for graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received. Stopping gracefully...");
            }));

            // Process initial batch (historical if enabled, otherwise only new)
            boolean isFirstRun = true;
            
            // Continuous polling loop
            boolean running = true;
            while (running) {
                try {
                    // On first run, use the processHistorical flag
                    // On subsequent runs, always process only new changes
                    boolean processHistoricalThisRun = isFirstRun && processHistorical;
                    
                    processBlobChanges(processor, dao, processHistoricalThisRun);
                    
                    if (isFirstRun) {
                        isFirstRun = false;
                        if (processHistorical) {
                            logger.info("Initial historical sync completed. Now monitoring for new changes only.");
                        }
                    }
                    
                    // Wait before next poll
                    logger.debug("Waiting {} seconds before next poll...", pollingInterval);
                    Thread.sleep(TimeUnit.SECONDS.toMillis(pollingInterval));
                } catch (InterruptedException e) {
                    logger.info("Interrupted. Shutting down...");
                    running = false;
                } catch (Exception e) {
                    logger.error("Error during polling cycle. Will retry in {} seconds", pollingInterval, e);
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(pollingInterval));
                    } catch (InterruptedException ie) {
                        logger.info("Interrupted during error recovery. Shutting down...");
                        running = false;
                    }
                }
            }

            logger.info("Sync service stopped");

        } catch (Exception e) {
            logger.error("Fatal error during sync", e);
            System.exit(1);
        }
    }

    /**
     * Process blob changes and update database
     * @param processor The blob change feed processor
     * @param dao The data access object
     * @param processHistorical If true, process all blobs; if false, only process new changes
     */
    private static void processBlobChanges(BlobChangeFeedProcessor processor, BlobChangeDao dao, boolean processHistorical) {
        try {
            // Get last processed timestamp
            OffsetDateTime lastProcessed = null;
            
            if (!processHistorical) {
                // Only process new changes - use last processed timestamp
                lastProcessed = dao.getLastProcessedTimestamp();
                if (lastProcessed != null) {
                    logger.debug("Processing changes since: {}", lastProcessed);
                } else {
                    logger.debug("No previous processing found. Processing all current blobs for initial sync.");
                }
            } else {
                // Process all blobs (historical + new) - don't use last processed timestamp
                logger.debug("Processing all blobs (historical data mode)");
            }
            
            // Get blob changes
            List<BlobChangeEvent> changes = processor.getBlobChanges(lastProcessed);

            // Filter only insert and update events
            List<BlobChangeEvent> insertAndUpdates = changes.stream()
                .filter(BlobChangeEvent::isInsertOrUpdate)
                .collect(Collectors.toList());

            if (insertAndUpdates.isEmpty()) {
                logger.debug("No new changes found");
                return;
            }

            logger.info("Processing {} insert/update events", insertAndUpdates.size());

            // Upsert changes to database
            int processed = 0;
            for (BlobChangeEvent event : insertAndUpdates) {
                try {
                    dao.upsertBlobChange(event);
                    processed++;
                    if (processed % 100 == 0) {
                        logger.info("Processed {} events", processed);
                    }
                } catch (Exception e) {
                    logger.error("Error processing blob change event: {}", event.getBlobName(), e);
                }
            }

            logger.info("Successfully processed {} blob change events", processed);
        } catch (Exception e) {
            logger.error("Error processing blob changes", e);
            throw new RuntimeException("Failed to process blob changes", e);
        }
    }

    private static void validateConfig(AppConfig config) {
        if (config.getBlobStorageConfig() == null) {
            throw new IllegalArgumentException("Blob storage configuration is required");
        }

        if (config.getDatabaseConfig() == null) {
            throw new IllegalArgumentException("Database configuration is required");
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
    }

    private static void printUsage() {
        System.out.println("Blob Change Feed to PostgreSQL Sync Utility");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java -jar blob-util-1.0.0.jar [options] [config-file]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -c, --config <file>    Path to configuration file");
        System.out.println("  -h, --help             Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar blob-util-1.0.0.jar");
        System.out.println("  java -jar blob-util-1.0.0.jar my-config.conf");
        System.out.println("  java -jar blob-util-1.0.0.jar -c /path/to/config.conf");
        System.out.println("  java -jar blob-util-1.0.0.jar --config application.conf");
        System.out.println();
        System.out.println("If no config file is specified, the utility will look for");
        System.out.println("'application.conf' in the current directory or use environment variables.");
    }
}

