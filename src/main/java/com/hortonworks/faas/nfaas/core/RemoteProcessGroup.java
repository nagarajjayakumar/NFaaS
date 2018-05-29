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


}
