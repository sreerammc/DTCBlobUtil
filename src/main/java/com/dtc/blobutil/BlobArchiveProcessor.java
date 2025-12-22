package com.dtc.blobutil;

import com.dtc.blobutil.config.AppConfig;
import com.dtc.blobutil.config.BlobStorageConfig;
import com.dtc.blobutil.config.ConfigLoader;
import com.dtc.blobutil.config.DatabaseConfig;
import com.dtc.blobutil.dao.BlobChangeDao;
import com.dtc.blobutil.processor.ArchiveFileProcessor;
import com.dtc.blobutil.processor.ArchiveFileProcessor.FileProcessingException;
import com.dtc.blobutil.processor.ArchiveFileProcessor.RecordCounts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Main utility class to process archived blob files and update record counts
 */
public class BlobArchiveProcessor {
    private static final Logger logger = LoggerFactory.getLogger(BlobArchiveProcessor.class);
    private static final int MAX_RETRIES = 3; // Maximum retry attempts for file processing

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

        logger.info("Starting Blob Archive Processor");
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

            // Initialize table (ensures columns exist)
            dao.initializeTable();

            // Initialize archive file processor
            BlobStorageConfig blobConfig = config.getBlobStorageConfig();
            if (blobConfig.getArchiveContainerName() == null || blobConfig.getArchiveContainerName().isEmpty()) {
                throw new IllegalArgumentException("Archive container name is required in configuration");
            }
            
            logger.info("Archive container: {}", blobConfig.getArchiveContainerName());
            ArchiveFileProcessor archiveProcessor = new ArchiveFileProcessor(blobConfig);

            // Get polling interval and processing delay
            long pollingInterval = blobConfig.getPollingIntervalSeconds();
            int minutesOld = blobConfig.getArchiveProcessingDelayMinutes();
            logger.info("Polling interval: {} seconds", pollingInterval);
            logger.info("Processing files older than {} minutes", minutesOld);
            logger.info("Starting continuous processing mode. Press Ctrl+C to stop.");

            // Add shutdown hook for graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received. Stopping gracefully...");
            }));

            // Continuous processing loop
            boolean running = true;
            while (running) {
                try {
                    processArchiveFiles(dao, archiveProcessor, minutesOld);
                    
                    // Wait before next poll
                    logger.debug("Waiting {} seconds before next poll...", pollingInterval);
                    Thread.sleep(TimeUnit.SECONDS.toMillis(pollingInterval));
                } catch (InterruptedException e) {
                    logger.info("Interrupted. Shutting down...");
                    running = false;
                } catch (Exception e) {
                    logger.error("Error during processing cycle. Will retry in {} seconds", pollingInterval, e);
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(pollingInterval));
                    } catch (InterruptedException ie) {
                        logger.info("Interrupted during error recovery. Shutting down...");
                        running = false;
                    }
                }
            }

            logger.info("Archive processor stopped");

        } catch (Exception e) {
            logger.error("Fatal error during archive processing", e);
            System.exit(1);
        }
    }

    /**
     * Process archive files older than specified minutes
     * @param dao The data access object
     * @param archiveProcessor The archive file processor
     * @param minutesOld Minimum age in minutes before processing
     */
    private static void processArchiveFiles(BlobChangeDao dao, ArchiveFileProcessor archiveProcessor, int minutesOld) {
        try {
            // Get blob names older than specified minutes that haven't been processed yet
            List<String> blobNames = dao.getBlobNamesOlderThan(minutesOld);

            if (blobNames.isEmpty()) {
                logger.debug("No files found that are older than {} minutes and need processing", minutesOld);
                return;
            }

            logger.info("Found {} files to process", blobNames.size());

            int processed = 0;
            int failed = 0;

            for (String blobName : blobNames) {
                try {
                    logger.debug("Processing file: {}", blobName);
                    
                    // Mark as processing
                    try {
                        dao.updateProcessingStatus(blobName, "PROCESSING");
                    } catch (Exception e) {
                        logger.warn("Failed to update status to PROCESSING for blob: {}", blobName, e);
                    }
                    
                    // Parse file from archive container with retry logic
                    RecordCounts counts = archiveProcessor.parseFileWithRetry(blobName, MAX_RETRIES);
                    
                    // Update database with record counts and mark as completed
                    dao.updateRecordCountsAndStatus(
                        blobName, 
                        counts.getTotalRecords(), 
                        counts.getDistinctRecords(), 
                        "COMPLETED"
                    );
                    
                    processed++;
                    if (processed % 10 == 0) {
                        logger.info("Processed {} files", processed);
                    }
                } catch (FileProcessingException e) {
                    failed++;
                    logger.error("Error processing file after {} retries: {}", MAX_RETRIES, blobName, e);
                    
                    // Mark as failed
                    try {
                        dao.updateProcessingStatus(blobName, "FAILED");
                    } catch (Exception statusException) {
                        logger.error("Failed to update status to FAILED for blob: {}", blobName, statusException);
                    }
                } catch (Exception e) {
                    failed++;
                    logger.error("Unexpected error processing file: {}", blobName, e);
                    
                    // Mark as failed
                    try {
                        dao.updateProcessingStatus(blobName, "FAILED");
                    } catch (Exception statusException) {
                        logger.error("Failed to update status to FAILED for blob: {}", blobName, statusException);
                    }
                }
            }

            logger.info("Processing complete. Processed: {}, Failed: {}", processed, failed);
        } catch (Exception e) {
            logger.error("Error processing archive files", e);
            throw new RuntimeException("Failed to process archive files", e);
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
        if (blobConfig.getArchiveContainerName() == null || blobConfig.getArchiveContainerName().isEmpty()) {
            throw new IllegalArgumentException("Archive container name is required");
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
        System.out.println("Blob Archive Processor");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java -cp blob-util-1.0.0.jar com.dtc.blobutil.BlobArchiveProcessor [options] [config-file]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -c, --config <file>    Path to configuration file");
        System.out.println("  -h, --help             Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -cp blob-util-1.0.0.jar com.dtc.blobutil.BlobArchiveProcessor");
        System.out.println("  java -cp blob-util-1.0.0.jar com.dtc.blobutil.BlobArchiveProcessor my-config.conf");
        System.out.println("  java -cp blob-util-1.0.0.jar com.dtc.blobutil.BlobArchiveProcessor -c /path/to/config.conf");
        System.out.println();
        System.out.println("This utility processes files from the archive container that are older than 10 minutes,");
        System.out.println("parses the JSON structure, counts total and distinct records, and updates the database.");
    }
}





