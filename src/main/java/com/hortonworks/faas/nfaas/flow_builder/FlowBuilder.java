package com.hortonworks.faas.nfaas.flow_builder;

import com.hortonworks.faas.nfaas.flow_builder.task.HiveDdlGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FlowBuilder {

    @Autowired
    private HiveDdlGenerator hiveDdlGenerator;

    public void doWork(FlowBuilderOptions fbo) {
        hiveDdlGenerator.doWork(fbo);
    }

    public void getProcessGroup(String pgName){

    }
}
