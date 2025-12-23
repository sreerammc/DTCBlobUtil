package com.dtc.blobutil.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Model for EventObject in ExportedEvents Objects array
 * Id, RecordTime, and SeqNo represent a unique record
 */
public class EventObject {
    @JsonProperty("Id")
    private Long id;
    
    @JsonProperty("Fullname")
    private String fullname;
    
    @JsonProperty("Severity")
    private String severity;
    
    @JsonProperty("ReceiptTime")
    private String receiptTime;
    
    @JsonProperty("RecordTime")
    private String recordTime;
    
    @JsonProperty("Category")
    private String category;
    
    @JsonProperty("User")
    private String user;
    
    @JsonProperty("AreaOfInterest")
    private String areaOfInterest;
    
    @JsonProperty("AlarmState")
    private String alarmState;
    
    @JsonProperty("Message")
    private String message;
    
    @JsonProperty("EncodedMessage")
    private String encodedMessage;
    
    @JsonProperty("SeqNo")
    private Long seqNo;

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

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getReceiptTime() {
        return receiptTime;
    }

    public void setReceiptTime(String receiptTime) {
        this.receiptTime = receiptTime;
    }

    public String getRecordTime() {
        return recordTime;
    }

    public void setRecordTime(String recordTime) {
        this.recordTime = recordTime;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getAreaOfInterest() {
        return areaOfInterest;
    }

    public void setAreaOfInterest(String areaOfInterest) {
        this.areaOfInterest = areaOfInterest;
    }

    public String getAlarmState() {
        return alarmState;
    }

    public void setAlarmState(String alarmState) {
        this.alarmState = alarmState;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getEncodedMessage() {
        return encodedMessage;
    }

    public void setEncodedMessage(String encodedMessage) {
        this.encodedMessage = encodedMessage;
    }

    public Long getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(Long seqNo) {
        this.seqNo = seqNo;
    }

    /**
     * Equals and hashCode based on Id, Fullname, RecordTime, and SeqNo (unique record identifier for events)
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventObject that = (EventObject) o;
        return Objects.equals(id, that.id) &&
               Objects.equals(fullname, that.fullname) &&
               Objects.equals(recordTime, that.recordTime) &&
               Objects.equals(seqNo, that.seqNo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, fullname, recordTime, seqNo);
    }
}
