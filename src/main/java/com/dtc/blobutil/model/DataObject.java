package com.dtc.blobutil.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Model for DataObject in Objects array
 * ID, Fullname, and Time represent a unique record
 */
public class DataObject {
    @JsonProperty("Id")
    private Long id;
    
    @JsonProperty("Fullname")
    private String fullname;
    
    @JsonProperty("Time")
    private String time;
    
    @JsonProperty("Value")
    private Double value;
    
    @JsonProperty("Reason")
    private Integer reason;
    
    @JsonProperty("State")
    private String state;
    
    @JsonProperty("Quality")
    private String quality;
    
    @JsonProperty("Units")
    private String units;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFullname() {
        return fullname;
    }

    public void setFullname(String fullname) {
        this.fullname = fullname;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Integer getReason() {
        return reason;
    }

    public void setReason(Integer reason) {
        this.reason = reason;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getQuality() {
        return quality;
    }

    public void setQuality(String quality) {
        this.quality = quality;
    }

    public String getUnits() {
        return units;
    }

    public void setUnits(String units) {
        this.units = units;
    }

    /**
     * Equals and hashCode based on Id, Fullname, and Time (unique record identifier)
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataObject that = (DataObject) o;
        return Objects.equals(id, that.id) &&
               Objects.equals(fullname, that.fullname) &&
               Objects.equals(time, that.time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, fullname, time);
    }
}





