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
package com.graphaware.module.es.mapping.json;
// TODO[Ian]: backport into JSONParentMapping

import com.fasterxml.jackson.annotation.JsonProperty;
import com.graphaware.common.log.LoggerFactory;
import com.graphaware.module.es.mapping.expression.NodeExpressions;
import com.graphaware.module.es.mapping.expression.RelationshipExpressions;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;

import java.util.*;

import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Str;
import org.neo4j.cypher.internal.frontend.v2_3.symbols.NodeType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.logging.Log;

public class DocumentMappingRepresentation {

    private static final Log LOG = LoggerFactory.getLogger(DocumentMappingRepresentation.class);
    
    private DocumentMappingDefaults defaults;

    private GraphDatabaseService database;

    public void setGraphDatabaseService(GraphDatabaseService database) {this.database = database;};

    @JsonProperty("parent_child_rels")
    private HashMap<String, GraphChildDocumentMapper> parentChildRels;

    @JsonProperty("node_mappings")
    private List<GraphDocumentMapper> nodeMappers;

    @JsonProperty("relationship_mappings")
    private List<GraphDocumentMapper> relationshipMappers;

    public DocumentMappingDefaults getDefaults() {
        return defaults;
    }

    public List<GraphDocumentMapper> getNodeMappers() {
        return nodeMappers;
    }

    public List<GraphDocumentMapper> getRelationshipMappers() {
        return relationshipMappers;
    }

    public List<BulkableAction<? extends JestResult>> createOrUpdateNode(NodeExpressions node) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();
        // TODO [Ian]: Check if node is child - look in nodes mappers? or somewhere else....
        for (GraphDocumentMapper mapper : nodeMappers) {
            if (mapper.supports(node)) {
                try {
                    DocumentRepresentation document = mapper.getDocumentRepresentation(node, defaults);
                    String json = document.getJson();
                    actions.add(new Index.Builder(json).index(document.getIndex()).type(document.getType()).id(document.getId()).build());
                } catch (Exception e) {
                    LOG.error("Error while creating or updating node", e);
                }

            }
        }

        return actions;
    }

    // TODO [Ian]: refactor - Can overload createOrUpdateNode + create+ update index
    // TODO [Ian]: Optimize
    public List<BulkableAction<? extends JestResult>> createOrUpdateChildNode(NodeExpressions node, GraphChildDocumentMapper child_maper) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        // TODO[Ian]: Store edge type in child document!

//        String id = node.getId();
//        getKeyProperty(node, defaults.getKeyProperty());

        // If child is true
        // Try statement

        try {
            // Get Id Here
            // Get child_mapper = type
            GraphDocumentMapper childNode = child_maper.getNode();

            // TODO: I think this is the test we want to do to verify everything is supported
//            childNode.supports(node); // This verifies the child / not the parent

            // Just store the end result maybe?
            DocumentRepresentation document = childNode.getDocumentRepresentation(node, defaults);

            // TODO[Ian] optimize this! Only request the id
            String docId = document.getId();
            String keyProperty = defaults.getKeyProperty();

            // /////////// Fetch parent ids//////////////////
            // TODO[Ian]: put into its own function!
            List<String> parentIds = new ArrayList<>();

            // Maybe get labels here?
            // TODO[Ian]: Try getType rather than get node - JK DON'T USE GET TYPE!!!!!!!!
            // GET TYPE RETURNS THE USER DEFINED TYPE...NOT NODE LABEL -- add new node label field in config
            // May look like its working if type is the same as label
            String childLabel = childNode.getType();

//            String childLabel = child_maper.getNodeType();
            // TODO[Ian]: rename get parent to more descriptive name
            String parentLabel = child_maper.getParent();

            // TODO[Ian]: Put in own function and test
            String query = "MATCH (n:" + parentLabel + ")" + child_maper.getRelated_by() + "(x:" + childLabel +
                           ") where x." + keyProperty + "= \"" + docId + "\" return n." + keyProperty;

            // Tentative plan - use cypher to find parents (easier than writing traversal)
            Result results = this.database.execute(query);

            Iterator<String> parent_ids = results.columnAs("n." + keyProperty);

            // Loop through and get parent ids
            while (parent_ids.hasNext()) {
                String parentUUID = parent_ids.next();
                parentIds.add(parentUUID);
            }
            ////////////////////////////////////
            for (String parentUuid : parentIds ) {
                String json = document.getJson();
                actions.add(new Index.Builder(json).index(document.getIndex()).type(document.getType())
                        .id(document.getId()).setParameter("parent", parentUuid).build());
            }


        } catch (Exception e) {
            LOG.error("Error while creating or updating node", e);
        }



        return actions;
    }

    public List<BulkableAction<? extends JestResult>> createOrUpdateRelationship(RelationshipExpressions relationship) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();
        // TODO: [IAN] add child check here (if there is a child relationship configured - executes child steps
        // If the relationship is part of a parent node find the two attached sides to update children
        if (parentChildRels.containsKey(relationship.getType())) {
            GraphChildDocumentMapper childNodeMapper = parentChildRels.get(relationship.getType());

            Node startNode;
            Node endNode;
            try ( Transaction tx = database.beginTx() )
            {
                startNode = this.database.getNodeById(relationship.getStartNodeGraphId());
                endNode = this.database.getNodeById(relationship.getEndNodeGraphId());

                NodeExpressions startNodeExpression = new NodeExpressions(startNode);
                NodeExpressions endNodeExpression = new NodeExpressions(endNode);

                // What do we do here about both ends (check root, check condition (ie that it is of type x - only send if that
                startNodeExpression.getLabels();
                endNodeExpression.getLabels();

                // Add the actions to the queue here. If the childNodeMapper doesn't support node types createOrUpdateChildNode will ignore it
                // TODO[Ian]: Should we check node types here?
                actions.addAll(createOrUpdateChildNode(startNodeExpression, childNodeMapper));
                actions.addAll(createOrUpdateChildNode(endNodeExpression, childNodeMapper));

                tx.success();
            } catch (Exception e) {
                LOG.error("Error while creating relationship: " + relationship.toString(), e);
            }

        }

        // Perform normal relationship checks here
        // TODO: if relationshipMappers is empty errors out!
        for (GraphDocumentMapper mapping : relationshipMappers) {
            if (mapping.supports(relationship)) {
                try {
                    DocumentRepresentation document = mapping.getDocumentRepresentation(relationship, defaults);
                    String json = document.getJson();
                    actions.add(new Index.Builder(json).index(document.getIndex()).type(document.getType()).id(document.getId()).build());
                } catch (Exception e) {
                    LOG.error("Error while creating relationship: " + relationship.toString(), e);
                }
            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> getDeleteRelationshipActions(RelationshipExpressions relationship) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (GraphDocumentMapper mapping : relationshipMappers) {
            if (mapping.supports(relationship)) {
                try {
                    DocumentRepresentation document = mapping.getDocumentRepresentation(relationship, defaults);
                    actions.add(new Delete.Builder(document.getId()).index(document.getIndex()).type(document.getType()).build());
                } catch (Exception e) {
                    LOG.error("Error while deleting relationship: " + relationship.toString(), e);
                }
            }
        }

        return actions;
    }

    // TODO [Ian]: Why use this over the createOrUpdate functions
    public List<BulkableAction<? extends JestResult>> updateNodeAndRemoveOldIndices(NodeExpressions before, NodeExpressions after) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();
        List<String> afterIndices = new ArrayList<>();
        for (DocumentRepresentation action : getNodeMappingRepresentations(after, defaults)) {
            afterIndices.add(action.getIndex() + "_" + action.getType());
            try {
                String json = action.getJson();
                actions.add(new Index.Builder(json).index(action.getIndex()).type(action.getType()).id(action.getId()).build());
            } catch (DocumentRepresentationException ex) {
                LOG.error("Error while adding action for node: " + before.toString(), ex);
            }
        }

        for (DocumentRepresentation representation : getNodeMappingRepresentations(before, defaults)) {
            if (!afterIndices.contains(representation.getIndex() + "_" + representation.getType())) {
                actions.add(new Delete.Builder(representation.getId()).index(representation.getIndex()).type(representation.getType()).build());
            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> updateRelationshipAndRemoveOldIndices(RelationshipExpressions before, RelationshipExpressions after) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();
        List<String> afterIndices = new ArrayList<>();

        for (DocumentRepresentation action : getRelationshipMappingRepresentations(after, defaults)) {
            afterIndices.add(action.getIndex() + "_" + action.getType());
            try {
                String json = action.getJson();
                actions.add(new Index.Builder(json).index(action.getIndex()).type(action.getType()).id(action.getId()).build());
            } catch (DocumentRepresentationException ex) {
                LOG.error("Error while adding update action for nodes: " + before.toString() + " -> " + after.toString(), ex);
            }            
        }

        for (DocumentRepresentation representation : getRelationshipMappingRepresentations(before, defaults)) {
            if (!afterIndices.contains(representation.getIndex() + "_" + representation.getType())) {
                actions.add(new Delete.Builder(representation.getId()).index(representation.getIndex()).type(representation.getType()).build());
            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> getDeleteNodeActions(NodeExpressions node) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();
        for (DocumentRepresentation documentRepresentation : getNodeMappingRepresentations(node, defaults)) {
            actions.add(new Delete.Builder(documentRepresentation.getId()).index(documentRepresentation.getIndex()).type(documentRepresentation.getType()).build());
        }

        return actions;
    }

    private List<DocumentRepresentation> getNodeMappingRepresentations(NodeExpressions nodeExpressions, DocumentMappingDefaults defaults) {
        List<DocumentRepresentation> docs = new ArrayList<>();
        for (GraphDocumentMapper mapper : getNodeMappers()) {
            if (mapper.supports(nodeExpressions)) {
                try {
                    DocumentRepresentation document = mapper.getDocumentRepresentation(nodeExpressions, defaults);
                    docs.add(document);
                } catch (Exception e) {
                    LOG.error("Error while getting document for node: " + nodeExpressions.toString(), e);
                }
            }
        }

        return docs;
    }

    private List<DocumentRepresentation> getRelationshipMappingRepresentations(RelationshipExpressions relationshipExpressions, DocumentMappingDefaults defaults) {
        List<DocumentRepresentation> docs = new ArrayList<>();
        for (GraphDocumentMapper mapper : getRelationshipMappers()) {
            if (mapper.supports(relationshipExpressions)) {
                try {
                    DocumentRepresentation document = mapper.getDocumentRepresentation(relationshipExpressions, defaults);
                    docs.add(document);
                } catch (Exception e) {
                    LOG.error("Error while getting document for relationship: " + relationshipExpressions.toString(), e);
                }
            }
        }

        return docs;
    }
}
