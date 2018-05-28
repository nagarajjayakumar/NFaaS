package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
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

public class FlowFileQueues {

    private static final Logger logger = LoggerFactory.getLogger(FlowFileQueues.class);

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
    FlowFileQueues(Environment env) {
        logger.info("Intialized FlowFileQueues !!! ");
        this.env = env;
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
    }

    /**
     * Place the request for the Deletion
     * https://localhost:8080/nifi-api/flowfile-queues/910e1c9c-015b-1000-a23d-97627b6ff030/drop-requests
     *
     * @param connection
     * @return
     */
    private DropRequestEntity placeRequestForDeletion(ConnectionEntity connection) {
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
    private DropRequestEntity deleteTheQueueContent(DropRequestEntity dre) {
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
}
