package com.hortonworks.faas.nfaas.flow_builder.task;

import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;
import com.hortonworks.faas.nfaas.orm.ActiveObjectDetailRepository;
import com.hortonworks.faas.nfaas.orm.ActiveObjectRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class HiveDmlGenerator implements Task {

    private static final Logger logger = LoggerFactory.getLogger(HiveDmlGenerator.class);
    public String task = "hive_dml_generator";

    private FlowBuilderOptions _fbo = null;

    @Autowired
    private ActiveObjectRepository activeObjectRepository;

    @Autowired
    private ActiveObjectDetailRepository activeObjectDetailRepository;

    public void doWork(FlowBuilderOptions fbo){
        logger.info("Inside the task " +task);
    }
}
