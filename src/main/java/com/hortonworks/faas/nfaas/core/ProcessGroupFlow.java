package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

public class ProcessGroupFlow {


    private static final Logger logger = LoggerFactory.getLogger(ProcessGroupFlow.class);

    Environment env;

    private String trasnsportMode = "http";
    private boolean nifiSecuredCluster = false;
    private String nifiServerHostnameAndPort = "localhost:9090";

    @Autowired
    Security security;

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    ProcessGroupFlow(Environment env) {

        logger.info("Intialized ProcessGroupFlow !!! ");
        this.env = env;
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
    }

    /**
     * This is the method which is used to get the root process group Flow
     * Entity
     *
     * @return
     */
    public ProcessGroupFlowEntity getRootProcessGroupFlowEntity() {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flow/process-groups/root";

        HttpEntity<ProcessGroupFlowEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                ProcessGroupFlowEntity.class, params);
        return response.getBody();
    }

    /**
     * this is the method to get the Latest process Group Entity
     *
     * @param processGroupFlowEntity
     * @return
     */
    @SuppressWarnings("unused")
    public ProcessGroupFlowEntity getLatestProcessGroupFlowEntity(ProcessGroupFlowEntity processGroupFlowEntity) {
        String pgId = processGroupFlowEntity.getProcessGroupFlow().getId();
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flow/process-groups/" + pgId + "/";
        HttpEntity<ProcessGroupFlowEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                ProcessGroupFlowEntity.class, params);
        return response.getBody();
    }

    /**
     * this is the method to get the Latest process flow Group Entity
     *
     * @param pgId
     * @return
     */
    public ProcessGroupFlowEntity getLatestProcessGroupFlowEntity(String pgId) {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flow/process-groups/" + pgId + "?recursive=true";
        HttpEntity<ProcessGroupFlowEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                ProcessGroupFlowEntity.class, params);
        return response.getBody();
    }

    /**
     * Call the NIFI rest api to start/stop the process group
     *
     * @param processGroupFlowEntity
     * @param state
     */
    public void startOrStopProcessGroupComponents(ProcessGroupFlowEntity processGroupFlowEntity, String state) {
        String pgId = processGroupFlowEntity.getProcessGroupFlow().getId();

        ScheduleComponentsEntity scheduleComponentsEntityReq = new ScheduleComponentsEntity();

        scheduleComponentsEntityReq.setId(pgId);
        scheduleComponentsEntityReq.setState(state);

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flow/process-groups/" + pgId + "/";

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders headers = security.getAuthorizationHeader();
        HttpEntity<ScheduleComponentsEntity> requestEntity = new HttpEntity<>(scheduleComponentsEntityReq, headers);

        HttpEntity<ProcessGroupFlowEntity> response = restTemplate.exchange(uri, HttpMethod.PUT, requestEntity,
                ProcessGroupFlowEntity.class, params);

        ProcessGroupFlowEntity resp = response.getBody();

        logger.debug(resp.toString());
    }

}
