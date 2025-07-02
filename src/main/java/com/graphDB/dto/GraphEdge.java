package com.graphDB.dto;

import lombok.Data;

@Data
public class GraphEdge {

    public String from;
    public String to;
    public String arrows = "to";

    public GraphEdge(String from, String to) {
        this.from = from;
        this.to = to;
    }
}
