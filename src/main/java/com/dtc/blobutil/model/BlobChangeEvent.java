package com.dtc.blobutil.model;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Represents a blob change event from the change feed
 */
public class BlobChangeEvent {
    private String blobName;
    private String eventType; // BlobCreated, BlobDeleted, BlobPropertiesUpdated, BlobMetadataUpdated, BlobRenamed, BlobTierChanged
    private String contentType;
    private Long contentLength;
    private String etag;
    private OffsetDateTime lastModified;
    private Map<String, String> metadata;
    private String url;
    private String versionId;
    private String snapshot;
    private String previousInfo; // For rename events

    public String getBlobName() {
        return blobName;
    }

    public void setBlobName(String blobName) {
        this.blobName = blobName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public Long getContentLength() {
        return contentLength;
    }

    public void setContentLength(Long contentLength) {
        this.contentLength = contentLength;
    }

    public String getEtag() {
        return etag;
    }

    public void setEtag(String etag) {
        this.etag = etag;
    }

    public OffsetDateTime getLastModified() {
        return lastModified;
    }

    public void setLastModified(OffsetDateTime lastModified) {
        this.lastModified = lastModified;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public String getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(String snapshot) {
        this.snapshot = snapshot;
    }

    public String getPreviousInfo() {
        return previousInfo;
    }

    public void setPreviousInfo(String previousInfo) {
        this.previousInfo = previousInfo;
    }

    public boolean isInsertOrUpdate() {
        return "BlobCreated".equals(eventType) || 
               "BlobPropertiesUpdated".equals(eventType) || 
               "BlobMetadataUpdated".equals(eventType);
    }
}








