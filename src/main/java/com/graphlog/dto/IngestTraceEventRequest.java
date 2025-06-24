package com.graphlog.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class IngestTraceEventRequest {

    public String traceId;
    public String serviceName;
    public String serviceVersion;
    public String hostname;
    public String eventType;
    public Map<String, Object> payload;
    public List<String> manualParentEventIds; // optional
}
