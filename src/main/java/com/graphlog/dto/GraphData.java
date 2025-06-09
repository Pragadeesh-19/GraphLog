package com.graphlog.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class GraphData {

    public List<GraphNode> nodes;
    public List<GraphEdge> edges;
}
