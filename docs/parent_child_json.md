## Parent Child Extension to Json File Mapping Definition
Parent child mapping is a work in progress. The theory behind it is that you can configure neo4j to sync parent child
relationships to elasticsearch. Parent child relationships are based on edges. So as edges are updated new parent child
relationships are created/updated.


Mapping syntax and instructions can be found in the json-mapper readme. This pull iteration adds a third option to the
config: parent_child_rels. This option is a dictionary of edge types. Each edge type can have one "parent" object. This
object must be one of the nodes defined in node_mappings. The child node is defined under the "node" option. The options
are identical to the node_mapping options. The related_by option is a  cypher query without the end nodes that connects the parent and child
nodes. so if (Person)-[Acted_In]-(Movie) include -[Acted_In]- and not (Person) - the parent node- and - movie - the child node.
Write the path with the parent node first and the child node at the end.

NOTE: for now type of the child node must be the same as the label or neo4j will not be able to find the correct path to sync
parent child.
// TODO: clarify related_by a tiny bit

Example mapping:
```
{
  "defaults": {
    "key_property": "uuid",
    "nodes_index": "neo4j-index-node",
    "relationships_index": "default-index-relationship",
    "include_remaining_properties": true
  },
  "node_mappings": [
    {
      "condition": "hasLabel('Person')",
      "type": "Person",
      "properties": {
        "search_string": "getProperty('firstName') + ' ' + getProperty('lastName')"
      }
    },
    {
      "condition": "hasLabel('Movie')",
      "type": "Movie_Ind"
    }
  ],
  "parent_child_rels": {
      "ACTED_IN": {
        "node": {
          "condition": "hasLabel('Movie')",
          "type": "Movie"
        },
        "parent": "Person",
        "related_by": "-[ACTED_IN]-"
      }
  }
}
```




Things to note
* Elasticsearch index has to be pre-configured manually before neo4j run (to tell es that there is a parent child relationship)
* The node "type" has to be the same as the neo4j label - in the future we will add a specific neo4j label option
* Edge values are not stored in parent child relationships
* You cannot have more than one type between children nodes and regular nodes

* Code needs to be cleaned up.


Things I want to do:
Integrate with jsonvalidator to check against nodes.

