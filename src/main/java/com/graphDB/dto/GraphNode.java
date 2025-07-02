package com.graphDB.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class GraphNode {

    public String id;
    public String label;
    public String title;
    public String group;
}
