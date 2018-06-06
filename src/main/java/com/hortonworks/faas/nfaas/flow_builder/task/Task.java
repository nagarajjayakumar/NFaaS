package com.hortonworks.faas.nfaas.flow_builder.task;

import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;

public interface Task {

    public void doWork(FlowBuilderOptions fbo);
}
