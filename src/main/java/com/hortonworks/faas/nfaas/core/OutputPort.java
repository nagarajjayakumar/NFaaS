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

public class OutputPort {


    private static final Logger logger = LoggerFactory.getLogger(OutputPort.class);

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
    OutputPort(Environment env) {
        logger.info("Intialized OutputPort !!! ");
        this.env = env;
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
    }

    /**
     * Call the NIFI rest api to start/stop the ports
     * https://localhost:8080/nifi-api/output-ports/{Port ID}
     *
     * @param portEntity
     * @param state
     * @return
     */
    private PortEntity startOrStopOutputPortEntity(PortEntity portEntity, String state) {
        String portId = portEntity.getComponent().getId();

        PortEntity portEntityReq = new PortEntity();
        PortDTO component = new PortDTO();
        portEntityReq.setComponent(component);
        RevisionDTO revision = new RevisionDTO();

        BeanUtils.copyProperties(portEntity.getRevision(), revision);

        portEntityReq.getComponent().setId(portId);
        portEntityReq.getComponent().setState(state);
        portEntityReq.setRevision(revision);

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/output-ports/" + portId + "/";

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders headers = security.getAuthorizationHeader();
        HttpEntity<PortEntity> requestEntity = new HttpEntity<>(portEntityReq, headers);

        HttpEntity<PortEntity> response = restTemplate.exchange(uri, HttpMethod.PUT, requestEntity, PortEntity.class,
                params);

        PortEntity resp = response.getBody();

        logger.debug(resp.toString());

        return resp;
    }
}
