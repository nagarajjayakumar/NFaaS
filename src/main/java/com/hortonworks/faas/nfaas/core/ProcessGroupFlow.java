package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
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
     * /flow/client-id
     * Method to get the client id from the flow api
     */

    public String getClientId() {

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flow/client-id";
        HttpEntity<String> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                String.class, params);
        return response.getBody();
    }


    /**
     *
     * GET /flow/registries
     * Gets the listing of available registries
     * Entity
     *
     * @return
     */
    public String getAvailableRegistry(String registryName) {

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<>(requestHeaders);

        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flow/registries";

        HttpEntity<LinkedHashMap> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                LinkedHashMap.class, params);

        LinkedHashMap map =  response.getBody();
        ArrayList registries = (ArrayList) map.get("registries");

        LinkedHashMap indRegistryMap = null;
        LinkedHashMap registry = null;

        String id = "";

        for(Object registryMap : registries){
            indRegistryMap = (LinkedHashMap)registryMap;
            registry = (LinkedHashMap) indRegistryMap.get("registry");
            if(registryName.equalsIgnoreCase(((String)registry.get("name")).toLowerCase()))
            {
                id = (String) registry.get("id");
                break;
            }
        }


        return id;
    }

    /**
     *
     * GET /flow/registries
     * Gets the listing of available registries
     * Entity
     *
     * @return
     */
    public RegistryClientsEntity getAvailableRegistry() {

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<>(requestHeaders);

        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flow/registries";

        HttpEntity<RegistryClientsEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                RegistryClientsEntity.class, params);

        return response.getBody();

    }


    /**
     * GET /flow/registries/{id}/buckets
     * Gets the buckets from the specified registry for the current use
     * Entity
     *
     * @return
     */
    public BucketsEntity getBucket(String registryId) {

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<>(requestHeaders);

        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flow/registries/"+registryId+"/buckets";

        HttpEntity<BucketsEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                BucketsEntity.class, params);
        return response.getBody();
    }

    /**
     *
     * GET  /flow/registries/{registry-id}/buckets/{bucket-id}/flows
     * Gets the flows from the specified registry and bucket for the current use
     * Entity
     *
     * @return
     */
    public VersionedFlowsEntity getAllFlowsFromRegistryAndBucket(String registryId, String bucketId) {

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flow/registries/"+registryId+"/buckets/"+bucketId+"/flows";

        HttpEntity<VersionedFlowsEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                VersionedFlowsEntity.class, params);
        return response.getBody();
    }


    /**
     *
     * /flow/registries/{registry-id}/buckets/{bucket-id}/flows/{flow-id}/versions
     * Gets the flow versions from the specified registry and bucket for the specified flow for the current user
     * Request
     * Entity
     *
     * @return
     */
    public VersionedFlowSnapshotMetadataSetEntity getFlowSnapShotFromRegistryAndBucketAndFlow(String registryId, String bucketId, String flowId) {

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flow/registries/"+registryId+"/buckets/"+bucketId+"/flows/"+flowId+"/versions";

        HttpEntity<VersionedFlowSnapshotMetadataSetEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                VersionedFlowSnapshotMetadataSetEntity.class, params);
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
