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

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.graphaware.common.log.LoggerFactory;
import com.graphaware.module.es.mapping.expression.NodeExpressions;
import com.graphaware.module.es.mapping.expression.RelationshipExpressions;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

/**
 * This mapping indexes all documents in the same ElasticSearch index.
 *
 * The node's neo4j labels are stored are ElasticSearch "type".
 * If a node has multiple labels, it is stored multiple times, once for each label.
 *
 * Relationships are not indexed.
 */
public class MimirMapping extends BaseMapping implements Mapping, TransactionAwareMapping {

    private static final Log LOG = LoggerFactory.getLogger(MimirMapping.class);

    // TODO: better message + make sure this doesn't break things
    @FieldSerializer.Optional ("null")
    private GraphDatabaseService database;

    public void setGraphDatabaseService(GraphDatabaseService database) {
        this.database = database;
    }

    public MimirMapping() {

    }

    @Override
    public List<BulkableAction<? extends JestResult>> deleteNode(NodeExpressions node) {
        String id = getKey(node);
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (String label : node.getLabels()) {
            actions.add(new Delete.Builder(id).index(getIndexFor(Node.class)).type(label).build());
        }

        return actions;
    }

    public static MimirMapping newInstance() {
        return new MimirMapping();
    }

    @Override
    public List<BulkableAction<? extends JestResult>> updateNode(NodeExpressions before, NodeExpressions after) {
        return createOrUpdateNode(after);
    }

    @Override
    public List<BulkableAction<? extends JestResult>> createNode(NodeExpressions node) {
        return createOrUpdateNode(node);
    }

    protected List<BulkableAction<? extends JestResult>> createOrUpdateNode(NodeExpressions node) {
        // This theoretically runs
        // TODO: v v v v v
        // Constraints:
        //     * Does not take relationships into account - so if relationships are modified, or given values surrounding nodes won't
        //          be updated
        //     * Parent/Child index must be configured by hand (for now)
        //     * Would need to find if the elasticsearch index is pre-configured (I think it is) - when would a reindex be implemented

        String id = getKey(node);

        // Build node id param list
        Map<String, Object> params = new HashMap<>();
        params.put("id", id);

        // Plan (write a version of the code that hardcodes everything
        // Plan ii Make everything configurable from file (or at least config) -- might have to modify json


        Map<String, Object> source = map(node);
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        //Dictionary mapping nodes to their parents using a cypher path
        Map<String, String> tempNodeMapping = new HashMap<String, String>();
        // return the uuid for processing by parent
        // TODO: how would finding parents on multiple types of relationships work? - We probably want to do something else
        // this.getKeyProperty() - index will get the property the key has been indexed with for searching
        tempNodeMapping.put("Movie", "MATCH (n:Person)-[]-(x:Movie) where x." + this.getKeyProperty() + "  = {id} return n.uuid");

        // For every label add the identical set of actions
        for (String label : node.getLabels()) {

            // Experiment here: if the label is person -> treat it as a normal node
            // if the label is anything else find the parent (ie a person)
            // Each label needs a mapping to the Person...

            // Parent Nodes
            if (label.equals("Person")) {
                // Index how you normally would
                actions.add(new Index.Builder(source).index(getIndexFor(Node.class)).type(label).id(id).build());
            } else if (tempNodeMapping.containsKey(label)) {
                // Get parent id's and iterate through

                // Tentative plan - use cypher to find parents (easier than writing traversal)
                Result results = this.database.execute(tempNodeMapping.get(label), params);

                Iterator<String> parent_ids = results.columnAs("uuid");

                // Loop through and get parent ids
                while (parent_ids.hasNext()) {
                    String ParentUUID = parent_ids.next();
                    actions.add(new Index.Builder(source).index(getIndexFor(Node.class)).type(label).id(id).type("child").setParameter("parent", ParentUUID).build());
                }

            } else {
                // Give up and just index how you normally would
                actions.add(new Index.Builder(source).index(getIndexFor(Node.class)).type(label).id(id).build());
            }
//            actions.add(new Index.Builder(source).index(getIndexFor(Node.class)).type(label).id(id).build());
        }

        return actions;
    }

    @Override
    public List<BulkableAction<? extends JestResult>> createRelationship(RelationshipExpressions relationship) {
        return createOrUpdateRelationship(relationship);
    }

    @Override
    public List<BulkableAction<? extends JestResult>> updateRelationship(RelationshipExpressions before, RelationshipExpressions after) {
        return createOrUpdateRelationship(after);
    }

    @Override
    public List<BulkableAction<? extends JestResult>> deleteRelationship(RelationshipExpressions r) {
        return Collections.singletonList(
                new Delete.Builder(getKey(r)).index(getIndexFor(Relationship.class)).type(r.getType()).build()
        );
    }

    protected List<BulkableAction<? extends JestResult>> createOrUpdateRelationship(RelationshipExpressions r) {
        return Collections.singletonList(
                new Index.Builder(map(r)).index(getIndexFor(Relationship.class)).type(r.getType()).id(getKey(r)).build()
        );
    }

    @Override
    public <T extends PropertyContainer> String getIndexFor(Class<T> searchedType) {
        return getIndexPrefix() + (searchedType.equals(Node.class) ? "-node" : "-relationship");
    }
}
