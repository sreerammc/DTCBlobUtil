package com.dtc.blobutil.processor;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.dtc.blobutil.config.BlobStorageConfig;
import com.dtc.blobutil.model.BlobChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Processor for reading blob change feed events
 * Note: Azure Blob Storage Change Feed is a feature that requires specific setup.
 * This implementation provides a polling-based approach to detect changes.
 * For production use with Change Feed enabled, you would use the Change Feed SDK.
 */
public class BlobChangeFeedProcessor {
    private static final Logger logger = LoggerFactory.getLogger(BlobChangeFeedProcessor.class);
    private final BlobContainerClient containerClient;
    private final BlobStorageConfig config;

    public BlobChangeFeedProcessor(BlobStorageConfig config) {
        this.config = config;
        this.containerClient = createBlobContainerClient(config);
    }

    /**
     * Create BlobContainerClient using connection string or managed identity
     */
    private BlobContainerClient createBlobContainerClient(BlobStorageConfig config) {
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
            logger.info("Authenticating with service principal: clientId={}, tenantId={}", 
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
                logger.error("Failed to create service principal credential. Please verify:", e);
                logger.error("  - Client ID format (should be a GUID like: 12345678-1234-1234-1234-123456789abc)");
                logger.error("  - Tenant ID: {}", config.getTenantId());
                logger.error("  - Account Name: {}", config.getAccountName());
                throw new RuntimeException("Service principal authentication failed. Check your clientId, tenantId, and clientSecret.", e);
            }
        } else {
            throw new IllegalArgumentException("Either connection string, managed identity, or service principal credentials must be provided");
        }

        BlobServiceClient blobServiceClient = builder.buildClient();
        return blobServiceClient.getBlobContainerClient(config.getContainerName());
    }

    /**
     * Get all blobs that were created or updated since the given timestamp
     * This is a polling-based approach. For true change feed, use Azure Change Feed SDK.
     */
    public List<BlobChangeEvent> getBlobChanges(OffsetDateTime since) {
        List<BlobChangeEvent> events = new ArrayList<>();
        
        try {
            ListBlobsOptions options = new ListBlobsOptions();
            // Note: We list all blobs and filter by timestamp in code below
            // For true change feed, use Azure Change Feed API

            logger.info("Scanning container {} for changes since {}", config.getContainerName(), since);
            
            for (BlobItem blobItem : containerClient.listBlobs(options, null)) {
                BlobClient blobClient = containerClient.getBlobClient(blobItem.getName());
                
                // Get blob properties to check last modified time
                BlobProperties properties = blobClient.getProperties();
                OffsetDateTime lastModified = properties.getLastModified();

                // Only process if modified after the given timestamp
                if (since == null || lastModified.isAfter(since) || lastModified.isEqual(since)) {
                    BlobChangeEvent event = createBlobChangeEvent(blobItem, properties, blobClient);
                    events.add(event);
                }
            }

            logger.info("Found {} blob changes", events.size());
        } catch (Exception e) {
            logger.error("Error reading blob changes", e);
            throw new RuntimeException("Failed to read blob changes", e);
        }

        return events;
    }

    /**
     * Create a BlobChangeEvent from blob item and properties
     */
    private BlobChangeEvent createBlobChangeEvent(BlobItem blobItem, BlobProperties properties, BlobClient blobClient) {
        BlobChangeEvent event = new BlobChangeEvent();
        event.setBlobName(blobItem.getName());
        
        // Determine event type based on blob state
        // In a real change feed, this would come from the change feed event
        if (blobItem.getProperties().getCreationTime() != null && 
            blobItem.getProperties().getLastModified() != null &&
            blobItem.getProperties().getCreationTime().equals(blobItem.getProperties().getLastModified())) {
            event.setEventType("BlobCreated");
        } else {
            event.setEventType("BlobPropertiesUpdated");
        }

        event.setContentType(properties.getContentType());
        event.setContentLength(properties.getBlobSize());
        event.setEtag(properties.getETag());
        event.setLastModified(properties.getLastModified());
        event.setMetadata(properties.getMetadata());
        event.setUrl(blobClient.getBlobUrl());
        event.setVersionId(properties.getVersionId());
        // Snapshot is not directly available from BlobProperties, set to null
        event.setSnapshot(null);

        return event;
    }

    /**
     * Get all current blobs (for initial sync)
     */
    public List<BlobChangeEvent> getAllBlobs() {
        return getBlobChanges(null);
    }
}


