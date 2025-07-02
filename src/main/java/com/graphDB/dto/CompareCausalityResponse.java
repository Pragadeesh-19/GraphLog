package com.graphDB.dto;

import com.graphDB.core.CausalRelationship;
import lombok.Builder;
import lombok.Data;

import java.util.Collections;
import java.util.List;

@Data
@Builder
public class CompareCausalityResponse {

    private String eventId1;
    private String eventId2;
    private CausalRelationship relationship;
    private boolean event1HappensBeforeEvent2_VC;
    private List<String> shortestPath1To2_ExplicitGraph;
    private List<String> shortestPath2To1_ExplicitGraph;
    private List<String> allCommonAncestors_ExplicitGraph;
    private List<String> nearestCommonAncestors_ExplicitGraph;

    public static CompareCausalityResponse empty() {
        return CompareCausalityResponse.builder()
                .shortestPath1To2_ExplicitGraph(Collections.emptyList())
                .shortestPath2To1_ExplicitGraph(Collections.emptyList())
                .allCommonAncestors_ExplicitGraph(Collections.emptyList())
                .nearestCommonAncestors_ExplicitGraph(Collections.emptyList())
                .build();
    }
}
