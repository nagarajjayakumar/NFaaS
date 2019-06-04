package com.hortonworks.faas.nfaas.graph;


import com.hortonworks.faas.nfaas.config.NifiType;
import com.hortonworks.faas.nfaas.xml.parser.FlowInfo;
import com.hortonworks.faas.nfaas.xml.parser.FlowParser;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FlowGraphCreator {
    public static void main(String[] args) throws Exception {
        // If you want to check your Gremlin version, uncomment the next line
        //System.out.println("Gremlin version is: " + Gremlin.version());

        FlowParser fp = new FlowParser();
        FlowInfo fi = fp.parse(new File("/Users/njayakumar/Downloads/flow.xml.gz"));
        // Create a new (empty) TinkerGrap
        TinkerGraph tg = TinkerGraph.open();

        // Create a Traversal source object
        GraphTraversalSource g = tg.traversal();



        List<ProcessGroupDTO> processGroups  = fi.getProcessGroups();

        GraphTraversal<Vertex, Vertex> gts =  g.addV(NifiType.PROCESS_GROUP.type)
                .property("isRoot", true)
                .property("pgName", fi.getRootGroupName())
                .property("pgId", fi.getRootGroupId())
                .as(fi.getRootGroupId());

        for(ProcessGroupDTO processGroup : processGroups){
            gts.addV(NifiType.PROCESS_GROUP.type)
                    .property("isRoot", false)
                    .property("pgName", processGroup.getName())
                    .property("pgId", processGroup.getId())
                    .as(processGroup.getId()).addE("parent").to(processGroup.getId()).from(processGroup.getParentGroupId());
        }

        gts.iterate();

//        g.addV("dadb47f2-016a-1000-19a1-7aaa9f611417").property("isRoot", true).as("dadb47f2-016a-1000-19a1-7aaa9f611417").iterate();
//                g.addV("dae2291a-016a-1000-985b-23f9ea93a026").property("isRoot", false).as("dae2291a-016a-1000-985b-23f9ea93a026").
//                addE("parent").from("dadb47f2-016a-1000-19a1-7aaa9f611417").to("dae2291a-016a-1000-985b-23f9ea93a026").
//                iterate();

//        // Add some nodes and vertices - Note the use of "iterate".
//        g.addV("airport").property("code", "AUS").as("aus").
//                addV("airport").property("code", "DFW").as("dfw").
//                addV("airport").property("code", "LAX").as("lax").
//                addV("airport").property("code", "JFK").as("jfk").
//                addV("airport").property("code", "ATL").as("atl").
//                addE("route").from("aus").to("dfw").
//                addE("route").from("aus").to("atl").
//                addE("route").from("atl").to("dfw").
//                addE("route").from("atl").to("jfk").
//                addE("route").from("dfw").to("jfk").
//                addE("route").from("dfw").to("lax").
//                addE("route").from("lax").to("jfk").
//                addE("route").from("lax").to("aus").
//                addE("route").from("lax").to("dfw").iterate();

        System.out.println(g);
        System.out.println(g.V().valueMap(true).toList());
        System.out.println(g.E().valueMap(true).toList());

        // Simple example of how to work with the results we get back from a query

        List<Map<Object, Object>> vm = new ArrayList<Map<Object, Object>>();

        vm = g.V().valueMap(true).toList();

        // Dislpay the code property as well as the label and id.
        for (Map m : vm) {
            System.out.println(((List) (m.get("pgName"))).get(0) + " " + m.get(T.id) + " " + m.get(T.label));
        }
        System.out.println();

        // Display the routes in the graph we just created.
        // Each path will include the vertex code values and the edge.

        List<Path> paths = new ArrayList<Path>();

        paths = g.V().outE().inV().path().by("pgId").by().toList();

        for (Path p : paths) {
            System.out.println(p.toString());
        }

        // Count how many vertices and edges we just created.
        // Using groupCount is overkill when we only have one label
        // but typically you will have more so this is a useful technique
        // to be aware of.
        System.out.println("\nWe just created");
        List verts = g.V().groupCount().by(T.label).toList();
        System.out.println(((Map) verts.get(0)).get(NifiType.PROCESS_GROUP.type) + " processorGroups");
        List edges = g.E().groupCount().by(T.label).toList();
        System.out.println(((Map) edges.get(0)).get("parent") + " Parents");

        // Note that we could also use the following code for a simple
        // case where we are only interested in specific labels.
        Long nv = g.V().hasLabel(NifiType.PROCESS_GROUP.type).count().next();
        Long ne = g.E().hasLabel("parent").count().next();
        System.out.println("The graph has " + nv + " process groups and " + ne + " connectivity");


        // Save the graph we just created as GraphML (XML) or GraphSON (JSON)
        try {
            // If you want to save the graph as GraphML uncomment the next line
            tg.io(IoCore.graphml()).writeGraph("mygraph.graphml");

            // If you want to save the graph as JSON uncomment the next line
            //tg.io(IoCore.graphson()).writeGraph("mygraph.json");
        } catch (IOException ioe) {
            System.out.println("Graph failed to save");
        }
    }
}