package com.hortonworks.faas.nfaas.flow_builder;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;

import java.util.List;

public class FlowBuilderOptions {

    @Parameter
    public List<String> parameters = Lists.newArrayList();

    @Parameter(names = "-namespace", description = "database namespace")
    public String namespace;

    @Parameter(names = "-package_id", description = "database package_id")
    public String package_id;

    @Parameter(names = "-db_object_name", description = "database object name")
    public String db_object_name;

    @Parameter(names = "-buckets", description = "table bucket name")
    public String buckets;


}
