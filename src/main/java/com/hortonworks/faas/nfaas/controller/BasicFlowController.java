package com.hortonworks.faas.nfaas.controller;

import com.hortonworks.faas.nfaas.core.*;
import com.hortonworks.faas.nfaas.core.helper.ProcessGroupFacadeHelper;
import com.hortonworks.faas.nfaas.core.helper.ProcessorGroupFlowFacadeHelper;
import com.hortonworks.faas.nfaas.core.helper.TemplateFacadeHelper;
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
    CommonService commonService;

    @Autowired
    Template template;

    @Autowired
    ProcessGroupFlow processGroupFlow;

    @Autowired
    ProcessGroup processGroup;


}
