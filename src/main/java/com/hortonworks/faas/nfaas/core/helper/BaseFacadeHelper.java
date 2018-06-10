package com.hortonworks.faas.nfaas.core.helper;

import com.hortonworks.faas.nfaas.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BaseFacadeHelper {

    private static final Logger logger = LoggerFactory.getLogger(BaseFacadeHelper.class);

    int WAIT_IN_SEC = 10;

    @Autowired
    ControllerService controllerService;

    @Autowired
    FlowFileQueue flowFileQueues;

    @Autowired
    ProcessGroup processGroup;

    @Autowired
    CommonService commonService;

    @Autowired
    Template template;

    @Autowired
    RemoteProcessGroup remoteProcessGroup;

    @Autowired
    ProcessGroupFlow processGroupFlow;

    @Autowired
    FlowFileQueue flowFileQueue;

    @Autowired
    InputPort inputPort;

    @Autowired
    OutputPort outputPort;

    @Autowired
    Processor processor;

    @Autowired
    Version version;

    @Autowired
    TemplateFacadeHelper templateFacadeHelper;

    @Autowired
    ProcessorGroupFlowFacadeHelper processorGroupFlowFacadeHelper;

    @Autowired
    ProcessGroupFacadeHelper processGroupFacadeHelper;


}
