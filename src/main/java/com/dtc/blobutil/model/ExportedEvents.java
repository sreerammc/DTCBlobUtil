package com.dtc.blobutil.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Model for ExportedEvents structure (for event files)
 */
public class ExportedEvents {
    @JsonProperty("Header")
    private Header header;
    
    @JsonProperty("Objects")
    private List<EventObject> objects;

    public Header getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    public List<EventObject> getObjects() {
        return objects;
    }

    public void setObjects(List<EventObject> objects) {
        this.objects = objects;
    }
}
