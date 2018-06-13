package com.hortonworks.faas.nfaas.controller;

import com.hortonworks.faas.nfaas.core.*;
import com.hortonworks.faas.nfaas.core.helper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BasicFlowController {

    private static final Logger logger = LoggerFactory.getLogger(BasicFlowController.class);

    int WAIT_IN_SEC = 10;

    @Autowired
    Security security;

    @Autowired
    TemplateFacadeHelper templateFacadeHelper;

    @Autowired
    ProcessorGroupFlowFacadeHelper processorGroupFlowFacadeHelper;

    @Autowired
    ProcessGroupFacadeHelper processGroupFacadeHelper;

    @Autowired
    FlowFileQueueFacadeHelper flowFileQueueFacadeHelper;

    @Autowired
    InputPortFacadeHelper inputPortFacadeHelper;

    @Autowired
    OutputPortFacadeHelper outputPortFacadeHelper;

    @Autowired
    ProcessorFacadeHelper processorFacadeHelper;

    @Autowired
    ControllerServiceFacadeHelper controllerServiceFacadeHelper;

    @Autowired
    RemoteProcessGroupFacadeHelper remoteProcessGroupFacadeHelper;

    @Autowired
    ConnectionFacadeHelper connectionFacadeHelper;

    @Autowired
    Connection connection;

    @Autowired
    CommonService commonService;

    @Autowired
    Template template;

    @Autowired
    ProcessGroupFlow processGroupFlow;

    @Autowired
    ProcessGroup processGroup;

    @Autowired
    FlowFileQueue flowFileQueue;

    @Autowired
    InputPort inputPort;

    @Autowired
    OutputPort outputPort;

    @Autowired
    Processor processor;

    @Autowired
    ControllerService controllerService;

    @Autowired
    RemoteProcessGroup remoteProcessGroup;




}
