package com.graphaware.module.es.mapping.json;


import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Wrap around a parent child relationship. Provides a container for holding child documents
 */
public class GraphChildDocumentMapper {
//    @JsonProperty("node")
    @JsonDeserialize(as=GraphDocumentMapper.class)
    private GraphDocumentMapper node;

    @JsonProperty
    private String parent;

    @JsonProperty
    // Cypher query relating the two nodes
    private String related_by;

    @JsonProperty
    private String nodeType;

    public String getNodeType() { return nodeType; }

    public String getParent() {
        return parent;
    }

    public String getRelated_by() {
        return related_by;
    }

    public GraphDocumentMapper getNode() { return node; }
}
