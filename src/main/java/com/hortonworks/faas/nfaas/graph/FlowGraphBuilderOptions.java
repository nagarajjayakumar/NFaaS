package com.hortonworks.faas.nfaas.graph;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;

import java.util.List;

public class FlowGraphBuilderOptions {

    @Parameter
    public List<String> parameters = Lists.newArrayList();

    @Parameter(names = "-nifiGraphMlPath", description = "NIFI Flow graph XML path")
    public String nifiGraphMlPath;
}
