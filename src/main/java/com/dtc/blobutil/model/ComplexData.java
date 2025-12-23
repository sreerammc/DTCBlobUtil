package com.dtc.blobutil.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Model for ComplexData JSON structure
 */
public class ComplexData {
    @JsonProperty("_name")
    private String name;
    
    @JsonProperty("_model")
    private String model;
    
    @JsonProperty("_timestamp")
    private Long timestamp;
    
    @JsonProperty("ExportedData")
    private ExportedData exportedData;
    
    @JsonProperty("ExportedEvents")
    private ExportedEvents exportedEvents;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public ExportedData getExportedData() {
        return exportedData;
    }

    public void setExportedData(ExportedData exportedData) {
        this.exportedData = exportedData;
    }
    
    public ExportedEvents getExportedEvents() {
        return exportedEvents;
    }

    public void setExportedEvents(ExportedEvents exportedEvents) {
        this.exportedEvents = exportedEvents;
    }
    
    /**
     * Check if this is an events file (has ExportedEvents)
     */
    public boolean isEventsFile() {
        return exportedEvents != null;
    }
    
    /**
     * Check if this is a data file (has ExportedData)
     */
    public boolean isDataFile() {
        return exportedData != null;
    }
}






