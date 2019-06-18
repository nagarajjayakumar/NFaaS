package com.hortonworks.faas.nfaas.controller;

import com.beust.jcommander.JCommander;
import com.hortonworks.faas.nfaas.graph.FlowGraphBuilderOptions;
import com.hortonworks.faas.nfaas.graph.FlowGraphLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

@RestController
public class NifiC2CController extends BasicFlowController {

    private static final Logger logger = LoggerFactory.getLogger(NifiC2CController.class);

    Environment env;

    private int WAIT_IN_SEC = 10;
    private String nifiServerHostnameAndPort = "localhost:9090";
    private String templateFileLocation = "classpath:Hello_NiFi_Web_Service.xml";
    private String templateFileURI = "https://cwiki.apache.org/confluence/download/attachments/57904847/Hello_NiFi_Web_Service.xml?version=1&modificationDate=1449369797000&api=v2";
    private String templateFileLoadFrom = "FILE";
    private boolean deleteQueueContent = false;
    private boolean undeployOnly = false;
    private String nifiUsername = "admin";
    private String nifiPassword = "BadPass#1";
    private String trasnsportMode = "http";
    private boolean nifiSecuredCluster = false;
    private boolean enableRPG = false;

    private final String statementDelim = ";";
    // "Authorization",
    // "Bearer
    // eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJIMjI4MzQ4IiwiaXNzIjoiTGRhcFByb3ZpZGVyIiwiYXVkIjoiTGRhcFByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiSDIyODM0OCIsImtpZCI6MSwiZXhwIjoxNDk0NDAzODM1LCJpYXQiOjE0OTQzNjA2MzV9.ztHHOr4uAnxa8Yx2qv5QV2b8grBxjHDx6vkUfYw00zQ"
    private final String authorizationHeaderKey = "Authorization";
    private final String authorizationHeaderValue = "Bearer ";

    private final String hive_extenal_table_location= "hdfs://AWHDP-QAHA/tmp/aw_hive_stg/";
    private final String  period= ".";
    private final String  fwd_slash = "/";
    private String nifiGraphMlPath = "/etc/hdfm/flow.xml.gz";

    @Autowired
    NifiC2CController(Environment env) {
        this.env = env;
        this.WAIT_IN_SEC = Integer.parseInt(env.getProperty("nifi.component.status.wait.sec"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
        this.templateFileLocation = env.getProperty("bootrest.templateFileLocation");
        this.templateFileURI = env.getProperty("bootrest.templateFileURI");
        this.templateFileLoadFrom = env.getProperty("bootrest.templateFileLoadFrom");
        this.deleteQueueContent = Boolean.parseBoolean(env.getProperty("bootrest.deleteQueueContent"));
        this.undeployOnly = Boolean.parseBoolean(env.getProperty("bootrest.undeployOnly"));
        this.nifiUsername = env.getProperty("bootrest.nifiUsername");
        this.nifiPassword = env.getProperty("bootrest.nifiPassword");
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");

        this.enableRPG = Boolean.parseBoolean(env.getProperty("bootrest.enableRPG"));
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiGraphMlPath = env.getProperty("nifi.graphml.path");

    }

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    FlowGraphLoader flowGraphLoader;

    /**
     * create hive table .. call the processor group and create the hive tables
     */

    @CrossOrigin
    @PreAuthorize("#oauth2.hasScope('read')")
    @RequestMapping(value = "/faas/loadnifigraph", produces = "application/json")
    public @ResponseBody
    String loadNifiGraph() {

        String loadNifiGraph = "{\"task\":\"load nifi graph done !\"}";

        FlowGraphBuilderOptions gbo = new FlowGraphBuilderOptions();

        List<String> args = new ArrayList<>();
        args.add("-nifiGraphMlPath");
        args.add(this.nifiGraphMlPath);

        JCommander.newBuilder()
                .addObject(gbo)
                .build()
                .parse(args.toArray(new String[0]));

        flowGraphLoader.loadGraph(gbo);



        return loadNifiGraph;
    }

}
