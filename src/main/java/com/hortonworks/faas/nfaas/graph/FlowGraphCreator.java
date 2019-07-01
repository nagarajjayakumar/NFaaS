package com.hortonworks.faas.nfaas.graph;


import com.hortonworks.faas.nfaas.config.NifiType;
import com.hortonworks.faas.nfaas.dto.FlowProcessor;
import com.hortonworks.faas.nfaas.xml.parser.FlowInfo;
import com.hortonworks.faas.nfaas.xml.parser.FlowParser;
import com.hortonworks.faas.nfaas.xml.util.NfaasStringUtil;
import com.hortonworks.faas.nfaas.xml.util.NfaasUtil;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertexProperty;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class FlowGraphCreator {
    public static void main(String[] args) throws Exception {
        // If you want to check your Gremlin version, uncomment the next line
        //System.out.println("Gremlin version is: " + Gremlin.version());

        FlowParser fp = new FlowParser();
        //FlowInfo fi = fp.parse(new File("/Users/njayakumar/Desktop/new-realflow.xml.gz"));
        FlowInfo fi = fp.parse(new File("/Users/njayakumar/Downloads/flow.xml.gz"));

        BaseConfiguration conf = new BaseConfiguration();
        conf.setProperty("gremlin.tinkergraph.vertexIdManager","LONG");
        conf.setProperty("gremlin.tinkergraph.edgeIdManager","LONG");
        conf.setProperty("gremlin.tinkergraph.vertexPropertyIdManager","LONG");
        // Create a new (empty) TinkerGrap
        TinkerGraph tg = TinkerGraph.open(conf);

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

        List<ProcessorDTO> processors = fi.getProcessors();

        if(true) {
            for (ProcessorDTO processor : processors) {
                Map<String, String> processorProperties = processor.getConfig().getProperties();
                gts.addV(NifiType.PROCESSOR.type)
                        .property("procName", processor.getName())
                        .property("procId", processor.getId());


                for (String propKey : processorProperties.keySet()) {
                    gts.property(propKey,processorProperties.get(propKey));
                }

                gts.as(processor.getId()).addE("parent").to(processor.getId()).from(processor.getParentGroupId());
            }

        }
        List<ConnectionDTO> connections = fi.getConnections();

        for(ConnectionDTO connection : connections){
            gts.addE("dependent").to(connection.getDestination().getGroupId()).from(connection.getSource().getGroupId());
        }

        gts.iterate();

//        g.addV("dadb47f2-016a-1000-19a1-7aaa9f611417").property("isRoot", true).as("dadb47f2-016a-1000-19a1-7aaa9f611417").iterate();
//                g.addV("dae2291a-016a-1000-985b-23f9ea93a026").property("isRoot", false).as("dae2291a-016a-1000-985b-23f9ea93a026").
//                addE("parent").from("dadb47f2-016a-1000-19a1-7aaa9f611417").to("dae2291a-016a-1000-985b-23f9ea93a026").
//                iterate();

//        // Add some nodes and vertices - Note the use of "iterate".
//        g.addV("airport").property("code", "AUS").as("aus").
//                addV("airport").property("code", "DFW").as("dfw").
//                addV("axmlirport").property("code", "LAX").as("lax").
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
        System.out.println("VALUEEEEEE MAP");
        System.out.println(g.V().valueMap(true).toList());
        System.out.println(g.E().valueMap(true).toList());

        // Simple example of how to work with the results we get back from a query

        List<Map<Object, Object>> vm = new ArrayList<Map<Object, Object>>();

        vm = g.V().valueMap(true).toList();

        // Dislpay the code property as well as the label and id.
        for (Map m : vm) {
            if(null != m.get("pgName"))
                System.out.println(((List) (m.get("pgName"))).get(0) + " " + m.get(T.id) + " " + m.get(T.label));
        }
        System.out.println();

        List<Map<String, Object>> propertyMap = new ArrayList<Map<String, Object>>();

        propertyMap = g.V().hasLabel(NifiType.PROCESSOR.type).propertyMap().toList();

        String searchString = "2019-05-21 11:56:55,313";
        FlowGraphService fgs = new FlowGraphService();
        // Dislpay the code property as well as the label and id.
        for (Map mp : propertyMap) {
            Iterator it = mp.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();

                List propValue = (ArrayList) pair.getValue();
                TinkerVertexProperty tvp = (TinkerVertexProperty) propValue.get(0);
                String tvpPropValue = tvp.value().toString();
                System.out.println((pair.getKey() + " = " + pair.getValue()));
                it.remove(); // avoids a ConcurrentModificationException
            }
        }

        // Display the routes in the graph we just created.
        // Each path will include the vertex code values and the edge.

        List<Path> paths = new ArrayList<Path>();

        //paths = g.V().outE().inV().path().by("pgId").by().toList();

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

        g.V().hasLabel(NifiType.PROCESSOR.type).
                has("procId","db1e4631-016a-1000-29b7-5401a7d27f8b").bothE("parent");

        // Save the graph we just created as GraphML (XML) or GraphSON (JSON)
        try {
            // If you want to save the graph as GraphML uncomment the next line
            tg.io(GraphMLIo.build()).writeGraph("nifi-graph.graphml");

            // If you want to save the graph as JSON uncomment the next line
            tg.io(GraphSONIo.build()).writeGraph("nifi-graph.json");
        } catch (IOException ioe) {
            System.out.println("Graph failed to save");
        }
    }
}