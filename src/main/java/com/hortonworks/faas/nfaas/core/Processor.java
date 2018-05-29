package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class Processor {

    private static final Logger logger = LoggerFactory.getLogger(Processor.class);

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
    Processor(Environment env) {
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

    /**
     * Call the NIFI rest api to delete the Processor
     * https://localhost:8080/nifi-api/processors/01213907-015b-1000-2760-95063f855d50?version=0&clientId=ca8915b0-30be-1fca-4c85-739031a5f7cf
     *
     * @param processor
     * @param state
     * @return
     */
    public ProcessorEntity deleteProcessorEntity(ProcessorEntity processor, String state) {

        String peId = processor.getId();

        // https://"+nifiServerHostnameAndPort+"/nifi-api/process-groups/a57d7d2a-86bd-4b43-357a-34abb1bd85d6?version=0&clientId=deaebc77-015b-1000-31ea-162516e98255
        String version = String.valueOf(commonService.getClientIdAndVersion(processor).getVersion());
        String clientId = String.valueOf(commonService.getClientIdAndVersion(processor).getClientId());

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/processors/" + peId + "?version="
                + version + "&clientId=" + clientId;

        Map<String, String> params = new HashMap<String, String>();

        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        ProcessorEntity resp = null;
        HttpEntity<ProcessorEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity,
                ProcessorEntity.class, params);

        resp = response.getBody();

        logger.debug(resp.toString());
        return resp;

    }

    /**
     * This is the method to get the latest Processor Entity
     *
     * @param processorEntity https://localhost:8080/nifi-api/processors/
     * @return
     */
    public ProcessorEntity getLatestProcessorEntity(ProcessorEntity processorEntity) {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/processors/" + processorEntity.getId()
                + "/";
        HttpEntity<ProcessorEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                ProcessorEntity.class, params);
        return response.getBody();
    }
}
