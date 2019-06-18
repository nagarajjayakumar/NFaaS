package com.hortonworks.faas.nfaas.graph;

import com.hortonworks.faas.nfaas.config.NifiType;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.IOException;
import java.util.List;

public class FlowGraphLoader
{
    private TinkerGraph tg;
    private GraphTraversalSource g;

    // -------------------------------------------------------------
    // Try to create a new graph and load the specified GraphML file
    // -------------------------------------------------------------
    public boolean loadGraph(String name)
    {
        // Make sure index ID values are set as LONG values.
        // If this is not done, when we try to sort results by vertex
        // ID later it will not sort the way you would expect it to.

        BaseConfiguration conf = new BaseConfiguration();
        conf.setProperty("gremlin.tinkergraph.vertexIdManager","LONG");
        conf.setProperty("gremlin.tinkergraph.edgeIdManager","LONG");
        conf.setProperty("gremlin.tinkergraph.vertexPropertyIdManager","LONG");

        // Create a new instance that uses this configuration.
        tg = TinkerGraph.open(conf) ;

        // Load the graph and time how long it takes.
        System.out.println("Loading " + name);
        long t1 = System.currentTimeMillis();
        System.out.println(t1);

        try
        {
            tg.io(GraphMLIo.build()).readGraph(name);
        }
        catch( IOException e )
        {
            System.out.println("ERROR - GraphML file not found or invalid.");
            return false;
        }

        long t2 = System.currentTimeMillis();
        System.out.println(t2 + "(" + (t2-t1) +")");
        System.out.println("Graph loaded\n");
        g = tg.traversal();
        return true;
    }

    // ----------------------------------------------
    // Display some information about the selected
    // number of airports. A value of -1 means select
    // all airports.
    // ----------------------------------------------
    public void listProcessGroup(int max)
    {
        if (max < -1 ) return;

        // Try to find the requested number of airports.
        // Note the use of the "__." and "Order" prefixes.
        List<Vertex> vlist =
                g.V().hasLabel(NifiType.PROCESS_GROUP.type).
                        order().by(__.id(),Order.incr).
                        limit(max).
                        toList();

        Long   id;   // Vertex ID
        Boolean isRoot; // 3 print is root
        String pgName; // 4 process group name
        String pgId; // process group id


        for (Vertex v : vlist)
        {
            id   = (Long)v.id();
            isRoot = (Boolean)v.values("isRoot").next();
            pgName = (String)v.values("pgName").next();
            pgId = (String)v.values("pgId").next();



            System.out.format("%5d %10s %30s %15s  \n",
                    id,isRoot,pgName,pgId);
        }
    }

    // ---------------------------------------
    // Try to load a graph and run a few tests
    // ---------------------------------------
    public static void main(String[] args)
    {
        int required = 10;
        boolean failed = false;

        try
        {
            if (args.length > 0) required = Integer.parseInt(args[0]);
        }
        catch (Exception e)
        {
            failed = true;
        }

        if (failed || required < -1)
        {
            System.out.println("Argument should be -1, 0 or any positive integer");
            System.exit(1);
        }

        FlowGraphLoader fgl = new FlowGraphLoader();

        if ( fgl.loadGraph("nifi-graph.graphml"))
        {
            fgl.listProcessGroup(required);
        }

    }
}
