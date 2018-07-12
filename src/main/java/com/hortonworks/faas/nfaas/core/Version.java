package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.VersionedFlowDTO;
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

import java.util.HashMap;
import java.util.Map;

@Configuration
public class Version {

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
    CommonService commonService;

    @Autowired
    Version(Environment env) {

        logger.info("Intialized Version !!! ");
        this.env = env;
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
    }


    /**
     * This is the method to save the Process group by ID
     * @param pgId
     * @param clientId
     * @param registryId
     * @param bucketId
     * @param version_num
     * @return
     */
    public VersionControlInformationEntity saveProcessGroupById(String pgId,
                                                                String clientId,
                                                                String flowName,
                                                                String registryId,
                                                                String bucketId,
                                                                long version_num, String comment){

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/versions/process-groups/" + pgId ;
        Map<String, String> params = new HashMap<String, String>();

        /*
            Create the process group entity object with the name ..
            to create the process group entity with the provided pgname
         */
        StartVersionControlRequestEntity startVersionControlRequestEntity =  new StartVersionControlRequestEntity();


        VersionedFlowDTO versionedFlow = new VersionedFlowDTO();
        versionedFlow.setRegistryId(registryId);
        versionedFlow.setBucketId(bucketId);
        versionedFlow.setFlowName(flowName);
        versionedFlow.setComments(comment);

        startVersionControlRequestEntity.setVersionedFlow(versionedFlow);
        /*
          Very critical to set the client Id and the inital version
          Otherwise the Httprequest will turn to a bad request.
         */
        RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId);
        revision.setVersion(version_num);

        startVersionControlRequestEntity.setProcessGroupRevision(revision);

        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<StartVersionControlRequestEntity> requestEntity = new HttpEntity<>(startVersionControlRequestEntity,requestHeaders);

        VersionControlInformationEntity resp = null;

        ResponseEntity<VersionControlInformationEntity> response = restTemplate.exchange(uri, HttpMethod.POST, requestEntity,
                VersionControlInformationEntity.class, params);

        resp = response.getBody();
        logger.debug(resp.toString());
        return resp;

    }

    /**
     * This is the method to save the Process group by ID
     * @param pge
     * @param clientId
     * @param registryId
     * @param bucketId
     * @param version_num
     * @return
     */
    public VersionControlInformationEntity updateProcessGroupById(ProcessGroupEntity pge,
                                                                String clientId,
                                                                String flowName,
                                                                String registryId,
                                                                String bucketId,
                                                                long version_num, String comment){

        String pgId = pge.getId();

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/versions/process-groups/" + pgId ;
        Map<String, String> params = new HashMap<String, String>();

        /*
            Create the process group entity object with the name ..
            to create the process group entity with the provided pgname
         */
        VersionedFlowSnapshotEntity versionedFlowSnapshotEntity =  new VersionedFlowSnapshotEntity();

        versionedFlowSnapshotEntity.setVersionedFlow(pge.getVersionedFlowSnapshot());
        versionedFlowSnapshotEntity.setRegistryId(registryId);
        versionedFlowSnapshotEntity.setUpdateDescendantVersionedFlows(true);

        /*
          Very critical to set the client Id and the inital version
          Otherwise the Httprequest will turn to a bad request.
         */
        RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId);
        revision.setVersion(version_num);

        versionedFlowSnapshotEntity.setProcessGroupRevision(revision);

        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<VersionedFlowSnapshotEntity> requestEntity = new HttpEntity<>(versionedFlowSnapshotEntity,requestHeaders);

        VersionControlInformationEntity resp = null;

        ResponseEntity<VersionControlInformationEntity> response = restTemplate.exchange(uri, HttpMethod.PUT, requestEntity,
                VersionControlInformationEntity.class, params);

        resp = response.getBody();
        logger.debug(resp.toString());
        return resp;

    }

    /**
     *
     GET
     /versions/process-groups/{id}
     Gets the Version Control information for a process group
     */

    public VersionControlInformationEntity getVersionControlInfoForPgId(String pgId){
        final String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/versions/process-groups/" + pgId ;
        Map<String, String> params = new HashMap<String, String>();

        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<>(requestHeaders);

        HttpEntity<VersionControlInformationEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                VersionControlInformationEntity.class, params);
        return response.getBody();

    }

    /**
     * This is the method to stop the version control for the Process group ID
     * @param pge
     * @return
     */

    public VersionControlInformationEntity stopVersionControlForPge(ProcessGroupEntity pge){

        String pgId = pge.getId();

        // https://"+nifiServerHostnameAndPort+"/nifi-api/versions/process-groups/a57d7d2a-86bd-4b43-357a-34abb1bd85d6?version=0&clientId=deaebc77-015b-1000-31ea-162516e98255
        String version = String.valueOf(commonService.getClientIdAndVersion(pge).getVersion());
        String clientId = String.valueOf(commonService.getClientIdAndVersion(pge).getClientId());

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/versions/process-groups/" + pgId + "?version="
                + version + "&clientId=" + clientId;

        Map<String, String> params = new HashMap<String, String>();

        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<>(requestHeaders);

        VersionControlInformationEntity resp = null;
        HttpEntity<VersionControlInformationEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity,
                VersionControlInformationEntity.class, params);

        resp = response.getBody();

        logger.debug(resp.toString());
        return resp;

    }
}
