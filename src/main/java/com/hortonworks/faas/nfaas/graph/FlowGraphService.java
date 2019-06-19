package com.hortonworks.faas.nfaas.graph;

import com.beust.jcommander.JCommander;
import com.hortonworks.faas.nfaas.config.NifiType;
import com.hortonworks.faas.nfaas.dto.FlowProcessGroup;
import com.hortonworks.faas.nfaas.dto.FlowProcessor;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class FlowGraphService {
    private static final Logger logger = LoggerFactory.getLogger(FlowGraphService.class);
    private TinkerGraph tg;
    private GraphTraversalSource g;

    // -------------------------------------------------------------
    // Try to create a new graph and load the specified GraphML file
    // -------------------------------------------------------------
    public boolean loadGraph(FlowGraphBuilderOptions gbo) {
        // Make sure index ID values are set as LONG values.
        // If this is not done, when we try to sort results by vertex
        // ID later it will not sort the way you would expect it to.

        BaseConfiguration conf = new BaseConfiguration();
        conf.setProperty("gremlin.tinkergraph.vertexIdManager", "LONG");
        conf.setProperty("gremlin.tinkergraph.edgeIdManager", "LONG");
        conf.setProperty("gremlin.tinkergraph.vertexPropertyIdManager", "LONG");

        // Create a new instance that uses this configuration.
        tg = TinkerGraph.open(conf);

        // Load the graph and time how long it takes.
        logger.debug("Loading " + gbo.nifiGraphMlPath);
        long t1 = System.currentTimeMillis();
        logger.debug("load start time " + t1);

        try {
            tg.io(GraphMLIo.build()).readGraph(gbo.nifiGraphMlPath);
        } catch (IOException e) {
            logger.debug("ERROR - GraphML file not found or invalid.");
            return false;
        }

        long t2 = System.currentTimeMillis();
        logger.debug(t2 + "(" + (t2 - t1) + ")");
        logger.debug("Graph loaded\n");
        g = tg.traversal();
        return true;
    }

    /***
     * this is the method to get the list of all process groups
     * @param max
     * @return
     */
    public List<FlowProcessGroup>  listProcessGroups(int max) {
        List<FlowProcessGroup> pgs = new ArrayList<>();

        if (max < -1) return pgs;

        if(g == null)
            throw new RuntimeException("FATAL :: Load the graph first !!!");

        // Try to find the requested number of process groups.
        // Note the use of the "__." and "Order" prefixes.
        List<Vertex> vlist =
                g.V().hasLabel(NifiType.PROCESS_GROUP.type).
                        order().by(__.id(), Order.incr).
                        limit(max).
                        toList();

        Long id;   // Vertex ID
        Boolean isRoot; // 3 print is root
        String pgName; // 4 process group name
        String pgId; // process group id

        FlowProcessGroup procGrp =  null;
        for (Vertex v : vlist) {
            procGrp = new FlowProcessGroup();
            id = (Long) v.id();
            isRoot = (Boolean) v.values("isRoot").next();
            pgName = (String) v.values("pgName").next();
            pgId = (String) v.values("pgId").next();


            logger.debug(String.format("%5d %10s %30s %15s  \n",
                    id, isRoot, pgName, pgId));

            procGrp.setId(id);
            procGrp.setPgId(pgId);
            procGrp.setPgName(pgName);
            procGrp.setRoot(isRoot);

            pgs.add(procGrp);
        }

        return pgs;
    }

    /***
     * this is the method to get the list of all processor based on the max supplied
     * @param max
     * @return
     */
    public List<FlowProcessor> listProcessors(int max) {

        List<FlowProcessor> procs = new ArrayList<>();

        if (max < -1) return procs;

        if(g == null)
            throw new RuntimeException("FATAL :: Load the graph first !!!");

        // Try to find the requested number of processor.
        // Note the use of the "__." and "Order" prefixes.
        List<Vertex> vlist =
                g.V().hasLabel(NifiType.PROCESSOR.type).
                        order().by(__.id(), Order.incr).
                        limit(max).
                        toList();

        Long id;   // Vertex ID
        String procName; // 3 Processor Name
        String procId; // 4 processor ID

        FlowProcessor proc = null;
        for (Vertex v : vlist) {

            id = (Long) v.id();
            procName = (String) v.values("procName").next();
            procId = (String) v.values("procId").next();
            logger.debug(String.format("%5d %10s %30s   \n",
                    id, procId, procName));

            proc = new FlowProcessor();
            proc.setId(id);
            proc.setProcId(procId);
            proc.setProcName(procName);

            procs.add(proc);

        }

        return procs;
    }


    /***
     * get the flow process group by ID
     * @param pgId
     * @return
     */
    public FlowProcessGroup getFlowProcessGroupById(String pgId) {
        int max = 5;
        if(g == null)
            throw new RuntimeException("FATAL :: Load the graph first !!!");

        // Try to find the requested number of airports.
        // Note the use of the "__." and "Order" prefixes.
        List<Vertex> vlist =
                g.V().hasLabel(NifiType.PROCESS_GROUP.type).has("pgId",pgId).
                        order().by(__.id(), Order.incr).
                        limit(max).
                        toList();

        Long id;   // Vertex ID
        Boolean isRoot; // 3 print is root
        String pgName; // 4 process group name
        String pgIdFromGraph; // process group id

        FlowProcessGroup procGrp =  new FlowProcessGroup();
        for (Vertex v : vlist) {
            procGrp = new FlowProcessGroup();
            id = (Long) v.id();
            isRoot = (Boolean) v.values("isRoot").next();
            pgName = (String) v.values("pgName").next();
            pgIdFromGraph = (String) v.values("pgId").next();
            logger.debug(String.format("%5d %10s %30s %15s  \n",
                    id, isRoot, pgName, pgId));
            procGrp.setId(id);
            procGrp.setPgId(pgIdFromGraph);
            procGrp.setPgName(pgName);
            procGrp.setRoot(isRoot);

        }

        return procGrp;
    }


    /***
     * this is the method to get the processor by ID
     * @param procId
     * @return
     */
    public FlowProcessor getProcessorById(String procId) {
        int max=5 ;
        if(g == null)
            throw new RuntimeException("FATAL :: Load the graph first !!!");

        // Try to find the requested number of processor.
        // Note the use of the "__." and "Order" prefixes.
        List<Vertex> vlist =
                g.V().hasLabel(NifiType.PROCESSOR.type).has("procId",procId).
                        order().by(__.id(), Order.incr).
                        limit(max).
                        toList();

        List<Edge> parentVlist =
                g.V().hasLabel(NifiType.PROCESSOR.type).has("procId",procId).bothE("parent").
                        order().by(__.id(), Order.incr).
                        limit(max).
                        toList();

        if(null == vlist || vlist.isEmpty())
            logger.debug("unable to find the processor by id " + procId);

        Long id;   // Vertex ID
        String procName; // 3 Processor Name
        String procIdFromGraph; // 4 processor ID

        FlowProcessor proc = new FlowProcessor();

        // take first proc can be part of one processor group
        // your assumption is 100 percentage

        Edge parentProcessGroup = parentVlist.get(0);

        for (Vertex v : vlist) {

            id = (Long) v.id();
            procName = (String) v.values("procName").next();
            procIdFromGraph = (String) v.values("procId").next();

            String pgId = parentProcessGroup.outVertex().value("pgId");
            String pgName = parentProcessGroup.outVertex().value("pgName");
            logger.debug(String.format("%5d %10s %30s %15s %30s  \n",
                    id, procId, procName,pgId,pgName));

            proc = new FlowProcessor();
            proc.setId(id);
            proc.setProcId(procIdFromGraph);
            proc.setProcName(procName);

            FlowProcessGroup fpg =  new FlowProcessGroup();
            fpg.setPgId(pgId);
            fpg.setPgName(pgName);

            proc.setFlowProcessGroup(fpg);
        }

        return proc;
    }
    // ---------------------------------------
    // Try to load a graph and run a few tests
    // ---------------------------------------
    public static void main(String[] args) {
        int required = 10;
        boolean failed = false;

        try {
            if (args.length > 0) required = Integer.parseInt(args[0]);
        } catch (Exception e) {
            failed = true;
        }

        if (failed || required < -1) {
            logger.debug("Argument should be -1, 0 or any positive integer");
            System.exit(1);
        }

        FlowGraphService fgl = new FlowGraphService();


        FlowGraphBuilderOptions gbo = new FlowGraphBuilderOptions();

        List<String> args1 = new ArrayList<>();
        args1.add("-nifiGraphMlPath");
        args1.add("nifi-graph.graphml");

        JCommander.newBuilder()
                .addObject(gbo)
                .build()
                .parse(args1.toArray(new String[0]));

        if (fgl.loadGraph(gbo)) {
            //fgl.listProcessGroups(required);
            //fgl.listProcessors(required);
            fgl.getProcessorById("db1e4631-016a-1000-29b7-5401a7d27f8b");
        }

    }

}
