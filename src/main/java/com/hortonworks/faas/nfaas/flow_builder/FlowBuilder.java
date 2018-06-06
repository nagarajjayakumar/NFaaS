package com.hortonworks.faas.nfaas.flow_builder;

import com.beust.jcommander.JCommander;

public class FlowBuilder {

    public static void main(String[] args) {

        FlowBuilderOptions fbo = new FlowBuilderOptions();
        JCommander.newBuilder()
                .addObject(fbo)
                .build()
                .parse(args);
    }
}
