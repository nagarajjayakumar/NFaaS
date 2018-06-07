package com.hortonworks.faas.nfaas.controller;

import com.beust.jcommander.JCommander;
import com.hortonworks.faas.nfaas.flow_builder.FlowBuilder;
import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.List;

/**
 * A class to interactions with the MySQL database using the ActiveObjectRepository class.
 *
 * @author njayakumar
 */
@Controller
public class FlowBuilderController {

    private static final Logger logger = LoggerFactory.getLogger(MetaDbController.class);

    @Autowired
    FlowBuilder flowBuilder;

    /**
     * /getaod  --> Return the Active Object detail
     */
    @CrossOrigin
    @PreAuthorize("#oauth2.hasScope('read')")
    @RequestMapping(value = "/faas/createhiveddl", produces = "application/json")
    public @ResponseBody
    String generateHiveDdl(String namespace,
                           String package_id,
                           String db_object_name,
                           String buckets,
                           String clustered_by) {

        FlowBuilderOptions fbo = new FlowBuilderOptions();

        List<String> args = new ArrayList<>();
        args.add("-namespace");
        args.add(namespace);
        args.add("-package_id");
        args.add(package_id);
        args.add("-db_object_name");
        args.add(db_object_name);
        args.add("-buckets");
        args.add(buckets);
        args.add("-clustered_by");
        args.add(clustered_by);

        JCommander.newBuilder()
                .addObject(fbo)
                .build()
                .parse(args.toArray(new String[0]));

        flowBuilder.doWork(fbo);
        return "createhiveddl done";
    }


}
