package com.dtc.blobutil.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Model for Header structure
 */
public class Header {
    @JsonProperty("SystemName")
    private String systemName;
    
    @JsonProperty("StartDate")
    private String startDate;
    
    @JsonProperty("EndDate")
    private String endDate;

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }
}





