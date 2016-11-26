package com.graphaware.module.es.mapping;
import com.graphaware.module.es.ElasticSearchConfiguration;
import com.graphaware.module.es.util.ServiceLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class ParentChildJsonFileMappingTest extends JsonFileMapping{

    protected static final String HOST = "localhost";
    protected static final String PORT = "9201";

//    protected GraphDatabaseService database;
    protected ElasticSearchConfiguration configuration;

    @Before
    public void setUp() {

    }

    @After
    public void tearDown() {

    }

    @Test
    public void testMapping() {
        Mapping mapping = ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();

        // Lets decide what type of mapping we are going to do!
        mappingConfig.put("file", "integration/mapping-basic-parent-child.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .withUri(HOST)
                .withPort(PORT);

//        assertEquals("uuid", configuration.getMapping().getKeyProperty());
//        assertEquals("default-index-node", ((JsonFileMapping)configuration.getMapping()).getMappingRepresentation().getDefaults().getDefaultNodesIndex());
//        assertEquals("default-index-relationship", ((JsonFileMapping)configuration.getMapping()).getMappingRepresentation().getDefaults().getDefaultRelationshipsIndex());
//        System.out.println((JsonFileMapping)configuration.getMapping());
//        System.out.println("TEST!!");
        // TODO[Ian]: write actual test here!
    }

}
