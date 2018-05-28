package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.PortEntity;
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

public class InputPort {

    private static final Logger logger = LoggerFactory.getLogger(InputPort.class);

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
    InputPort(Environment env) {
        logger.info("Intialized InputPort !!! ");
        this.env = env;
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
    }

    /**
     * Call the NIFI rest api to start/stop the ports
     * https://localhost:8080/nifi-api/input-ports/{Port ID}
     *
     * @param portEntity
     * @param state
     * @return
     */
    public PortEntity startOrStopInputPortEntity(PortEntity portEntity, String state) {
        String portId = portEntity.getComponent().getId();

        PortEntity portEntityReq = new PortEntity();
        PortDTO component = new PortDTO();
        portEntityReq.setComponent(component);
        RevisionDTO revision = new RevisionDTO();

        BeanUtils.copyProperties(portEntity.getRevision(), revision);

        portEntityReq.getComponent().setId(portId);
        portEntityReq.getComponent().setState(state);
        portEntityReq.setRevision(revision);

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/input-ports/" + portId;

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders headers = security.getAuthorizationHeader();
        HttpEntity<PortEntity> requestEntity = new HttpEntity<>(portEntityReq, headers);

        HttpEntity<PortEntity> response = restTemplate.exchange(uri, HttpMethod.PUT, requestEntity, PortEntity.class,
                params);

        PortEntity resp = response.getBody();
        logger.debug(resp.toString());

        return resp;
    }

    /**
     * Call the NIFI rest api to delete the input port
     * https://localhost:8080/nifi-api/input-ports/b3b53358-a6f5-1789-332e-fa552a5fe01a?version=0&clientId=ca8915b0-30be-1fca-4c85-739031a5f7cf
     *
     * @param portEntity
     * @param state
     * @return
     */
    public PortEntity deleteInputPortEntity(PortEntity portEntity, String state) {

        String peId = portEntity.getId();

        // https://"+nifiServerHostnameAndPort+"/nifi-api/process-groups/a57d7d2a-86bd-4b43-357a-34abb1bd85d6?version=0&clientId=deaebc77-015b-1000-31ea-162516e98255
        String version = String.valueOf(commonService.getClientIdAndVersion(portEntity).getVersion());
        String clientId = String.valueOf(commonService.getClientIdAndVersion(portEntity).getClientId());

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/input-ports/" + peId + "?version="
                + version + "&clientId=" + clientId;

        Map<String, String> params = new HashMap<String, String>();

        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        PortEntity resp = null;
        HttpEntity<PortEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity, PortEntity.class,
                params);

        resp = response.getBody();

        logger.debug(resp.toString());
        return resp;

    }

    /**
     * This is the method to get the latest Input Port Entity
     *
     * @param portEntity
     * @return
     */
    public PortEntity getLatestInputPortEntity(PortEntity portEntity) {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/input-ports/" + portEntity.getId() + "/";
        HttpEntity<PortEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity, PortEntity.class,
                params);
        return response.getBody();
    }
}
