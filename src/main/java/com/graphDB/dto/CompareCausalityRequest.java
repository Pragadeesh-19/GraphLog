package com.graphDB.dto;

import lombok.Data;

@Data
public class CompareCausalityRequest {
    private String eventId1;
    private String eventId2;
}
