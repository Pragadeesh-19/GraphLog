package com.graphlog.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class IngestEventRequest {

    public String entityId;
    public String eventType;
    public Map<String, Object> payload;
    public List<String> causalParentEventIds;
}
