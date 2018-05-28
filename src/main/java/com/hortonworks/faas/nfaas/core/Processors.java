package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

public class Processors {

    private static final Logger logger = LoggerFactory.getLogger(Processors.class);

    Environment env;

    private String trasnsportMode = "http";
    private boolean nifiSecuredCluster = false;
    private String nifiServerHostnameAndPort = "localhost:9090";

    @Autowired
    Security security;

    @Autowired
    ProcessGroupFlow processGroupFlow;

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    CommonService commonService;

    @Autowired
    Processors(Environment env) {
        logger.info("Intialized Processors !!! ");
        this.env = env;
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
    }

    /**
     * Call the NIFI rest api to start/stop the Processor
     * https://localhost:8080/nifi-api/processors/
     *
     * @param processor
     * @param state
     * @return
     */
    public ProcessorEntity startOrStopProcessorEntity(ProcessorEntity processor, String state) {

        String processorId = processor.getComponent().getId();

        ProcessorEntity processorEntityReq = new ProcessorEntity();
        ProcessorDTO component = new ProcessorDTO();
        processorEntityReq.setComponent(component);
        RevisionDTO revision = new RevisionDTO();

        BeanUtils.copyProperties(processor.getRevision(), revision);

        processorEntityReq.getComponent().setId(processorId);
        processorEntityReq.getComponent().setState(state);
        processorEntityReq.setRevision(revision);

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/processors/" + processorId + "/";

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders headers = security.getAuthorizationHeader();
        HttpEntity<ProcessorEntity> requestEntity = new HttpEntity<>(processorEntityReq, headers);

        HttpEntity<ProcessorEntity> response = restTemplate.exchange(uri, HttpMethod.PUT, requestEntity,
                ProcessorEntity.class, params);

        ProcessorEntity resp = response.getBody();

        logger.debug(resp.toString());

        return resp;
    }
}
