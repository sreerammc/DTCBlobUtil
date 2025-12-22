package com.dtc.blobutil.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Model for ExportedData structure
 */
public class ExportedData {
    @JsonProperty("Header")
    private Header header;
    
    @JsonProperty("Objects")
    private List<DataObject> objects;

    public Header getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    public List<DataObject> getObjects() {
        return objects;
    }

    public void setObjects(List<DataObject> objects) {
        this.objects = objects;
    }
}





