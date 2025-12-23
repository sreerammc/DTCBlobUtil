package com.dtc.blobutil.processor;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.dtc.blobutil.config.BlobStorageConfig;
import com.dtc.blobutil.model.ComplexData;
import com.dtc.blobutil.model.DataObject;
import com.dtc.blobutil.model.EventObject;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Processor for reading and parsing files from archive container
 */
public class ArchiveFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ArchiveFileProcessor.class);
    private final BlobContainerClient archiveContainerClient;
    private final ObjectMapper objectMapper;

    public ArchiveFileProcessor(BlobStorageConfig config) {
        this.archiveContainerClient = createArchiveContainerClient(config);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        // Configure Jackson to be more lenient with JSON parsing
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        this.objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    }

    /**
     * Create BlobContainerClient for archive container using connection string or managed identity
     */
    private BlobContainerClient createArchiveContainerClient(BlobStorageConfig config) {
        if (config.getArchiveContainerName() == null || config.getArchiveContainerName().isEmpty()) {
            throw new IllegalArgumentException("Archive container name is required");
        }

        BlobServiceClientBuilder builder = new BlobServiceClientBuilder();

        if (config.getConnectionString() != null && !config.getConnectionString().isEmpty()) {
            builder.connectionString(config.getConnectionString());
        } else if (config.isUseManagedIdentity()) {
            TokenCredential credential = new DefaultAzureCredentialBuilder().build();
            builder.credential(credential)
                   .endpoint(String.format("https://%s.blob.core.windows.net", config.getAccountName()));
        } else if (config.getClientId() != null && config.getClientSecret() != null) {
            if (config.getTenantId() == null || config.getTenantId().isEmpty()) {
                throw new IllegalArgumentException("Tenant ID is required when using service principal authentication");
            }
            if (config.getAccountName() == null || config.getAccountName().isEmpty()) {
                throw new IllegalArgumentException("Account name is required when using service principal authentication");
            }
            logger.info("Authenticating with service principal for archive container: clientId={}, tenantId={}", 
                config.getClientId(), config.getTenantId());
            try {
                TokenCredential credential = new ClientSecretCredentialBuilder()
                    .tenantId(config.getTenantId())
                    .clientId(config.getClientId())
                    .clientSecret(config.getClientSecret())
                    .build();
                builder.credential(credential)
                       .endpoint(String.format("https://%s.blob.core.windows.net", config.getAccountName()));
            } catch (Exception e) {
                logger.error("Failed to create service principal credential for archive container", e);
                throw new RuntimeException("Service principal authentication failed for archive container", e);
            }
        } else {
            throw new IllegalArgumentException("Either connection string, managed identity, or service principal credentials must be provided");
        }

        BlobServiceClient blobServiceClient = builder.buildClient();
        return blobServiceClient.getBlobContainerClient(config.getArchiveContainerName());
    }

    /**
     * Read and parse a file from the archive container with retry logic
     * @param blobName The name of the blob to read
     * @param maxRetries Maximum number of retry attempts (default: 3)
     * @return RecordCounts containing total and distinct record counts
     * @throws FileProcessingException if all retries are exhausted
     */
    public RecordCounts parseFileWithRetry(String blobName, int maxRetries) throws FileProcessingException {
        int attempt = 0;
        FileProcessingException lastException = null;
        
        while (attempt <= maxRetries) {
            try {
                if (attempt > 0) {
                    logger.info("Retry attempt {} of {} for blob: {}", attempt, maxRetries, blobName);
                    // Wait before retry (exponential backoff: 1s, 2s, 4s)
                    Thread.sleep((long) Math.pow(2, attempt - 1) * 1000);
                }
                
                return parseFile(blobName);
            } catch (FileProcessingException e) {
                lastException = e;
                attempt++;
                if (attempt <= maxRetries) {
                    logger.warn("Attempt {} failed for blob: {}. Will retry...", attempt, blobName, e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FileProcessingException("Interrupted while retrying for blob: " + blobName, e);
            }
        }
        
        // All retries exhausted
        if (lastException != null) {
            logger.error("All {} retry attempts exhausted for blob: {}", maxRetries, blobName, lastException);
            throw new FileProcessingException("Failed to parse file after " + maxRetries + " retries: " + blobName, lastException);
        } else {
            // This should never happen, but handle it just in case
            throw new FileProcessingException("Failed to parse file after " + maxRetries + " retries: " + blobName);
        }
    }

    /**
     * Read and parse a file from the archive container
     * @param blobName The name of the blob to read
     * @return RecordCounts containing total and distinct record counts
     * @throws FileProcessingException if file cannot be read or parsed
     */
    public RecordCounts parseFile(String blobName) throws FileProcessingException {
        try {
            BlobClient blobClient = archiveContainerClient.getBlobClient(blobName);
            
            if (!blobClient.exists()) {
                logger.warn("Blob does not exist in archive container: {}", blobName);
                throw new FileProcessingException("Blob does not exist in archive container: " + blobName);
            }

            logger.debug("Reading blob from archive container: {}", blobName);
            
            try (InputStream inputStream = blobClient.openInputStream()) {
                ComplexData complexData;
                try {
                    complexData = objectMapper.readValue(inputStream, ComplexData.class);
                } catch (com.fasterxml.jackson.core.JsonParseException e) {
                    logger.error("JSON parse error for blob {} at line {}, column {}: {}", 
                        blobName, e.getLocation().getLineNr(), e.getLocation().getColumnNr(), e.getMessage(), e);
                    throw new FileProcessingException("JSON parse error for blob: " + blobName + 
                        " at line " + e.getLocation().getLineNr() + ", column " + e.getLocation().getColumnNr() + 
                        ". Error: " + e.getMessage(), e);
                } catch (com.fasterxml.jackson.databind.JsonMappingException e) {
                    logger.error("JSON mapping error for blob {}: {}", blobName, e.getMessage(), e);
                    if (e.getPath() != null && !e.getPath().isEmpty()) {
                        logger.error("Error path in JSON: {}", e.getPath());
                    }
                    throw new FileProcessingException("JSON mapping error for blob: " + blobName + 
                        ". Path: " + (e.getPath() != null ? e.getPath().toString() : "unknown") + 
                        ". Error: " + e.getMessage(), e);
                } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                    logger.error("JSON processing error for blob {}: {}", blobName, e.getMessage(), e);
                    throw new FileProcessingException("JSON processing failed for blob: " + blobName + ". Error: " + e.getMessage(), e);
                } catch (java.io.IOException e) {
                    logger.error("IO error reading blob {}: {}", blobName, e.getMessage(), e);
                    throw new FileProcessingException("IO error reading blob: " + blobName + ". Error: " + e.getMessage(), e);
                } catch (Exception e) {
                    logger.error("Unexpected error during JSON parsing for blob {}: {}", blobName, e.getMessage(), e);
                    throw new FileProcessingException("Failed to parse JSON for blob: " + blobName + ". Error: " + e.getMessage(), e);
                }
                
                if (complexData == null) {
                    logger.warn("Invalid file structure for blob: {} - ComplexData is null", blobName);
                    throw new FileProcessingException("Invalid file structure for blob: " + blobName + " - ComplexData is null");
                }
                
                // Check if this is an events file or data file
                if (complexData.isEventsFile()) {
                    return processEventsFile(complexData, blobName);
                } else if (complexData.isDataFile()) {
                    return processDataFile(complexData, blobName);
                } else {
                    logger.warn("Invalid file structure for blob: {} - Neither ExportedData nor ExportedEvents found", blobName);
                    throw new FileProcessingException("Invalid file structure for blob: " + blobName + " - Neither ExportedData nor ExportedEvents found");
                }
            }
        } catch (FileProcessingException e) {
            throw e; // Re-throw FileProcessingException as-is
        } catch (Exception e) {
            logger.error("Error parsing file from archive container: {}", blobName, e);
            throw new FileProcessingException("Failed to parse file: " + blobName, e);
        }
    }

    /**
     * Process data file (iris_data format) with ExportedData
     */
    private RecordCounts processDataFile(ComplexData complexData, String blobName) throws FileProcessingException {
        if (complexData.getExportedData() == null) {
            logger.warn("Invalid file structure for blob: {} - ExportedData is null", blobName);
            throw new FileProcessingException("Invalid file structure for blob: " + blobName + " - ExportedData is null");
        }

        List<DataObject> objects = complexData.getExportedData().getObjects();
        
        if (objects == null || objects.isEmpty()) {
            logger.debug("No objects found in data file: {}", blobName);
            return new RecordCounts(0, 0);
        }

        int totalRecords = objects.size();
        
        // Count distinct records (based on Id, Fullname, Time)
        Set<DataObject> distinctObjects = new HashSet<>(objects);
        int distinctRecords = distinctObjects.size();

        logger.debug("Parsed data file {}: total records={}, distinct records={}", 
            blobName, totalRecords, distinctRecords);

        return new RecordCounts(totalRecords, distinctRecords);
    }
    
    /**
     * Process events file (ExportedEvents format) with deduplication based on Id, RecordTime, SeqNo
     */
    private RecordCounts processEventsFile(ComplexData complexData, String blobName) throws FileProcessingException {
        if (complexData.getExportedEvents() == null) {
            logger.warn("Invalid file structure for blob: {} - ExportedEvents is null", blobName);
            throw new FileProcessingException("Invalid file structure for blob: " + blobName + " - ExportedEvents is null");
        }

        List<EventObject> events = complexData.getExportedEvents().getObjects();
        
        if (events == null || events.isEmpty()) {
            logger.debug("No events found in events file: {}", blobName);
            return new RecordCounts(0, 0);
        }

        int totalRecords = events.size();
        
        // Count distinct records (based on Id, RecordTime, SeqNo)
        Set<EventObject> distinctEvents = new HashSet<>(events);
        int distinctRecords = distinctEvents.size();

        logger.debug("Parsed events file {}: total records={}, distinct records={}", 
            blobName, totalRecords, distinctRecords);

        return new RecordCounts(totalRecords, distinctRecords);
    }

    /**
     * Custom exception for file processing errors
     */
    public static class FileProcessingException extends Exception {
        public FileProcessingException(String message) {
            super(message);
        }

        public FileProcessingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Inner class to hold record count results
     */
    public static class RecordCounts {
        private final int totalRecords;
        private final int distinctRecords;

        public RecordCounts(int totalRecords, int distinctRecords) {
            this.totalRecords = totalRecords;
            this.distinctRecords = distinctRecords;
        }

        public int getTotalRecords() {
            return totalRecords;
        }

        public int getDistinctRecords() {
            return distinctRecords;
        }
    }
}






