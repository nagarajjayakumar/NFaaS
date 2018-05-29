package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupsEntity;
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

public class RemoteProcessGroup {

    private static final Logger logger = LoggerFactory.getLogger(RemoteProcessGroup.class);

    Environment env;

    private String trasnsportMode = "http";
    private boolean nifiSecuredCluster = false;
    private String nifiServerHostnameAndPort = "localhost:9090";
    private boolean enableRPG = false;

    @Autowired
    Security security;

    @Autowired
    ProcessGroupFlow processGroupFlow;

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    CommonService commonService;

    @Autowired
    RemoteProcessGroup(Environment env) {
        logger.info("Intialized RemoteProcessGroup !!! ");
        this.env = env;
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
        this.enableRPG = Boolean.parseBoolean(env.getProperty("bootrest.enableRPG"));
    }

    /**
     * This is the method which us used to enable or disable the remote process
     * Group components
     * http://localhost:8080/nifi-api/remote-process-groups/f2fe8ad1-015b-1000-64fd-caf013397f4a
     *
     * @param remoteProcessGroupEntity
     * @param state
     */
    public RemoteProcessGroupEntity enableOrDisableRemoteProcessGroupComponents(
            RemoteProcessGroupEntity remoteProcessGroupEntity, String state) {

        String rpgId = remoteProcessGroupEntity.getComponent().getId();

        RemoteProcessGroupEntity remoteProcessGroupEntityReq = new RemoteProcessGroupEntity();

        RemoteProcessGroupDTO component = new RemoteProcessGroupDTO();
        remoteProcessGroupEntityReq.setComponent(component);

        RevisionDTO revision = new RevisionDTO();

        BeanUtils.copyProperties(remoteProcessGroupEntity.getRevision(), revision);

        remoteProcessGroupEntityReq.getComponent().setId(rpgId);
        remoteProcessGroupEntityReq.getComponent().setTransmitting(Boolean.valueOf(state));
        remoteProcessGroupEntityReq.setRevision(revision);

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/remote-process-groups/" + rpgId + "/";

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders headers = security.getAuthorizationHeader();
        HttpEntity<RemoteProcessGroupEntity> requestEntity = new HttpEntity<>(remoteProcessGroupEntityReq, headers);

        HttpEntity<RemoteProcessGroupEntity> response = restTemplate.exchange(uri, HttpMethod.PUT, requestEntity,
                RemoteProcessGroupEntity.class, params);

        RemoteProcessGroupEntity resp = response.getBody();

        logger.debug(resp.toString());

        return resp;
    }


    /**
     * This is the method which is used to get the get the remote process groups
     * for the remote process grp ID ..
     *
     * @param rpgeId
     * @return /remote-process-groups/{id} Gets a remote process group
     */
    public RemoteProcessGroupEntity getLatestRemoteProcessGroupEntity(String rpgeId) {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/remote-process-groups/" + rpgeId + "/";
        HttpEntity<RemoteProcessGroupEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                RemoteProcessGroupEntity.class, params);
        return response.getBody();
    }

    /**
     * This is the method which is used to get the remote process groups for the
     * Process GROUP ID .. /process-groups/{id}/remote-process-groups
     *
     * @param pgId
     * @return
     */
    public RemoteProcessGroupsEntity getLatestRemoteProcessGroupsEntity(String pgId) {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/process-groups/" + pgId
                + "/remote-process-groups/";
        HttpEntity<RemoteProcessGroupsEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                RemoteProcessGroupsEntity.class, params);
        return response.getBody();
    }


    /**
     * This is the method which is used to delete the remote process group
     * componets
     * http://localhost:8080/nifi-api/remote-process-groups/f2fe8ad1-015b-1000-64fd-caf013397f4a?version=6&clientId=f2fe58d9-015b-1000-f615-591b1d0de0c2
     *
     * @param remoteProcessGroupEntity
     */
    public RemoteProcessGroupEntity deleteRemoteProcessGroupComponents(
            RemoteProcessGroupEntity remoteProcessGroupEntity) {

        logger.info(
                "Delete Remote Group Service Entity Starts --> " + remoteProcessGroupEntity.getComponent().getName());
        String rpgeId = remoteProcessGroupEntity.getId();

        // https://"+nifiServerHostnameAndPort+"/nifi-api/controller-services/b369d993-48ae-4c0e-5ddc-ac8b8f316c4b?version=2&clientId=deaebc77-015b-1000-31ea-162516e98255
        String version = String.valueOf(commonService.getClientIdAndVersion(remoteProcessGroupEntity).getVersion());
        String clientId = String.valueOf(commonService.getClientIdAndVersion(remoteProcessGroupEntity).getClientId());

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/remote-process-groups/" + rpgeId
                + "?version=" + version + "&clientId=" + clientId;

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        HttpEntity<RemoteProcessGroupEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity,
                RemoteProcessGroupEntity.class, params);

        RemoteProcessGroupEntity resp = response.getBody();

        logger.debug(resp.toString());
        logger.info("Delete Remote Group Entity Ends --> " + remoteProcessGroupEntity.getComponent().getName());
        return resp;

    }

    public boolean isEnableRPG(){
       return this.enableRPG;
    }


}
