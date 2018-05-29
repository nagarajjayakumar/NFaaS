package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FlowFileQueue {

    private static final Logger logger = LoggerFactory.getLogger(FlowFileQueue.class);

    Environment env;

    private String trasnsportMode = "http";
    private boolean nifiSecuredCluster = false;
    private String nifiServerHostnameAndPort = "localhost:9090";
    private boolean deleteQueueContent = false;

    @Autowired
    Security security;

    @Autowired
    ProcessGroupFlow processGroupFlow;

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    CommonService commonService;

    @Autowired
    FlowFileQueue(Environment env) {
        logger.info("Intialized FlowFileQueue !!! ");
        this.env = env;
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
        this.deleteQueueContent = Boolean.parseBoolean(env.getProperty("bootrest.deleteQueueContent"));
    }

    /**
     * Place the request for the Deletion
     * https://localhost:8080/nifi-api/flowfile-queues/910e1c9c-015b-1000-a23d-97627b6ff030/drop-requests
     *
     * @param connection
     * @return
     */
    public DropRequestEntity placeRequestForDeletion(ConnectionEntity connection) {
        String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flowfile-queues/" + connection.getId()
                + "/drop-requests";

        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        ResponseEntity<DropRequestEntity> response = restTemplate.exchange(uri, HttpMethod.POST, requestEntity,
                DropRequestEntity.class);
        DropRequestEntity dre = response.getBody();

        return dre;
    }

    /**
     * Actual method to delete the queue content
     *
     * @param dre
     * @return
     */
    public DropRequestEntity deleteTheQueueContent(DropRequestEntity dre) {
        final String uri = dre.getDropRequest().getUri();

        Map<String, String> params = new HashMap<String, String>();

        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        HttpEntity<DropRequestEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity,
                DropRequestEntity.class, params);

        DropRequestEntity resp = response.getBody();

        logger.debug(resp.toString());

        return resp;
    }


    /**
     * Delete the queue content for the process group entity
     *
     * @param pge
     */
    public void deleteTheQueueContent(ProcessGroupEntity pge) {
        if (deleteQueueContent == false)
            throw new RuntimeException("Queues Are Not Empty.. Please flush the queus manually before deletion...");

        ProcessGroupFlowEntity pgfe = processGroupFlow.getLatestProcessGroupFlowEntity(pge.getId());
        Set<ProcessGroupEntity> processGroups = pgfe.getProcessGroupFlow().getFlow().getProcessGroups();

        int queuedCount = 0;

        for (ProcessGroupEntity processGroupEntity : processGroups) {
            queuedCount = Integer.parseInt(processGroupEntity.getStatus().getAggregateSnapshot().getQueuedCount().replaceAll(",", ""));

            if (queuedCount > 0) {
                deleteTheQueueContent(processGroupEntity);
            }
        }

        Set<ConnectionEntity> connections = pgfe.getProcessGroupFlow().getFlow().getConnections();

        int queuedCountInConnections = 0;
        DropRequestEntity dre = null;
        for (ConnectionEntity connection : connections) {
            queuedCountInConnections = Integer.parseInt(connection.getStatus().getAggregateSnapshot().getQueuedCount().replaceAll(",", ""));
            if (queuedCountInConnections > 0) {
                dre = placeRequestForDeletion(connection);
                dre = deleteTheQueueContent(dre);
            }
        }

    }
}
