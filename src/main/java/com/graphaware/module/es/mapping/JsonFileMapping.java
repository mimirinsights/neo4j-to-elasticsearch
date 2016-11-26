/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.es.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.graphaware.common.log.LoggerFactory;
import com.graphaware.module.es.mapping.json.DocumentMappingRepresentation;
import com.graphaware.module.es.mapping.expression.NodeExpressions;
import com.graphaware.module.es.mapping.expression.RelationshipExpressions;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.logging.Log;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class JsonFileMapping implements Mapping, TransactionAwareMapping {

    private static final Log LOG = LoggerFactory.getLogger(JsonFileMapping.class);

    private static final String DEFAULT_KEY_PROPERTY = "uuid";
    private static final String FILE_PATH_KEY = "file";

    private DocumentMappingRepresentation mappingRepresentation;

    private GraphDatabaseService database;

    protected String keyProperty;

    @Override
    public void configure(Map<String, String> config) {
        if (!config.containsKey(FILE_PATH_KEY)) {
            throw new RuntimeException("Configuration is missing the " + FILE_PATH_KEY + "key");
        }
        try {
            String file = new ClassPathResource(config.get("file")).getFile().getAbsolutePath();
            mappingRepresentation = new ObjectMapper().readValue(new File(file), DocumentMappingRepresentation.class);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read json mapping file", e);
        }
    }

    public DocumentMappingRepresentation getMappingRepresentation() {
        return mappingRepresentation;
    }

    public List<BulkableAction<? extends JestResult>> createNode(NodeExpressions node) {
        return mappingRepresentation.createOrUpdateNode(node);
    }

    public List<BulkableAction<? extends JestResult>> createRelationship(RelationshipExpressions relationship) {
        return mappingRepresentation.createOrUpdateRelationship(relationship);
    }

    public List<BulkableAction<? extends JestResult>> updateRelationship(RelationshipExpressions before, RelationshipExpressions after) {
        return mappingRepresentation.createOrUpdateRelationship(after);
    }

    @Override
    public List<BulkableAction<? extends JestResult>> updateNode(NodeExpressions before, NodeExpressions after) {
        return mappingRepresentation.createOrUpdateNode(after);
    }

    public List<BulkableAction<? extends JestResult>> deleteNode(NodeExpressions node) {
        return mappingRepresentation.getDeleteNodeActions(node);
    }

    public List<BulkableAction<? extends JestResult>> deleteRelationship(RelationshipExpressions relationship) {
        return mappingRepresentation.getDeleteRelationshipActions(relationship);
    }

    @Override
    public void createIndexAndMapping(JestClient client) throws Exception {

    }

    @Override
    public <T extends PropertyContainer> String getIndexFor(Class<T> searchedType) {
        return null;
    }

    @Override
    public String getKeyProperty() {
        return mappingRepresentation.getDefaults().getKeyProperty() != null ? mappingRepresentation.getDefaults().getKeyProperty() : DEFAULT_KEY_PROPERTY;
    }

    @Override
    public boolean bypassInclusionPolicies() {
        return true;
    }

    @Override
    public void setGraphDatabaseService(GraphDatabaseService database) {
        this.database = database;
        mappingRepresentation.setGraphDatabaseService(database);
    }
}
