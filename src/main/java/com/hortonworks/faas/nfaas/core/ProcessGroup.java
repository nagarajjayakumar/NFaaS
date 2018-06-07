package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.dto.ProcessGroupDTO;
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
import java.util.Set;

@Configuration
public class ProcessGroup {

    private static final Logger logger = LoggerFactory.getLogger(ProcessGroup.class);

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
    ProcessGroup(Environment env) {
        this.env = env;
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
    }

    /**
     * this is the method to get the Latest process Group Entity
     *
     * @param pgId
     * @return
     */
    public ProcessGroupEntity getLatestProcessGroupEntity(String pgId) {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/process-groups/" + pgId + "/";
        HttpEntity<ProcessGroupEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                ProcessGroupEntity.class, params);
        return response.getBody();
    }


    /**
     * delete the process group ...
     *
     * @param pge
     * @param state
     * @return
     */
    public ProcessGroupEntity deleteProcessGroup(ProcessGroupEntity pge, String state) {

        String pgId = pge.getId();

        // https://"+nifiServerHostnameAndPort+"/nifi-api/process-groups/a57d7d2a-86bd-4b43-357a-34abb1bd85d6?version=0&clientId=deaebc77-015b-1000-31ea-162516e98255
        String version = String.valueOf(commonService.getClientIdAndVersion(pge).getVersion());
        String clientId = String.valueOf(commonService.getClientIdAndVersion(pge).getClientId());

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/process-groups/" + pgId + "?version="
                + version + "&clientId=" + clientId;

        Map<String, String> params = new HashMap<String, String>();

        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        ProcessGroupEntity resp = null;
        HttpEntity<ProcessGroupEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity,
                ProcessGroupEntity.class, params);

        resp = response.getBody();

        logger.debug(resp.toString());
        return resp;

    }

    /**
     * create the process group ...
     *
     * @param pge
     * @param pgName
     * @return
     */
    public ProcessGroupEntity createProcessGroup(ProcessGroupEntity pge, String pgName) {

        String pgId = pge.getId();

        // https://"+nifiServerHostnameAndPort+"/nifi-api/process-groups/a57d7d2a-86bd-4b43-357a-34abb1bd85d6?version=0&clientId=deaebc77-015b-1000-31ea-162516e98255
        String version = String.valueOf(commonService.getClientIdAndVersion(pge).getVersion());
        String clientId = String.valueOf(commonService.getClientIdAndVersion(pge).getClientId());

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/process-groups/" + pgId + "/process-groups?version="
                + version + "&clientId=" + clientId;

        Map<String, String> params = new HashMap<String, String>();

        /*
            Create the process group entity object with the name ..
            to create the process group entity with the provided pgname
         */
        ProcessGroupEntity reqPge =  new ProcessGroupEntity();
        ProcessGroupDTO component = new ProcessGroupDTO();
        component.setName(pgName);
        reqPge.setComponent(component);

        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(reqPge,requestHeaders);

        ProcessGroupEntity resp = null;

        ResponseEntity<ProcessGroupEntity> response = restTemplate.exchange(uri, HttpMethod.POST, requestEntity,
                ProcessGroupEntity.class,params);

        ProcessGroupEntity resp_pge = response.getBody();

        resp = response.getBody();
        logger.debug(resp.toString());
        return resp_pge;

    }

    /**
     *
     * this is the method to get the variable registry for the process group entity
     * http://localhost:8080/nifi-api/process-groups/da2ad18e-b984-35ec-828b-30de2fbf0a4f/variable-registry
     * @param pge
     * @return
     */

    public VariableRegistryEntity getVariableRegistry(ProcessGroupEntity pge){
        String pgId = pge.getId();

        // http://localhost:8080/nifi-api/process-groups/da2ad18e-b984-35ec-828b-30de2fbf0a4f/variable-registry
        String version = String.valueOf(commonService.getClientIdAndVersion(pge).getVersion());
        String clientId = String.valueOf(commonService.getClientIdAndVersion(pge).getClientId());

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/process-groups/" + pgId + "/variable-registry?version="
                + version + "&clientId=" + clientId;

        Map<String, String> params = new HashMap<String, String>();

        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        VariableRegistryEntity resp = null;
        HttpEntity<VariableRegistryEntity> response = restTemplate.exchange(uri, HttpMethod.GET, requestEntity,
                VariableRegistryEntity.class, params);

        resp = response.getBody();

        logger.debug(resp.toString());
        return resp;
    }


    public VariableRegistryEntity updateVariableRegistry(String pgId, VariableRegistryEntity vre){

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/process-groups/" + pgId + "/variable-registry";

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders headers = security.getAuthorizationHeader();
        HttpEntity<VariableRegistryEntity> requestEntity = new HttpEntity<>(vre, headers);

        HttpEntity<VariableRegistryEntity> response = restTemplate.exchange(uri, HttpMethod.PUT, requestEntity,
                VariableRegistryEntity.class, params);

        VariableRegistryEntity resp = response.getBody();

        logger.debug(resp.toString());

        return resp;
    }

    /**
     * get All Controller Services By Process Group
     *
     * @param pgId
     * @return
     */
    public ControllerServicesEntity getAllControllerServicesByProcessGroup(String pgId) {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flow/process-groups/" + pgId
                + "/controller-services/";
        HttpEntity<ControllerServicesEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                ControllerServicesEntity.class, params);
        return response.getBody();
    }

    /**
     * This is the method which is used to get the working process Group ID
     *
     * @param pgfe
     * @return
     */
    @SuppressWarnings("unused")
    private ProcessGroupEntity getAboutToDeployProcessGroupId(ProcessGroupFlowEntity pgfe) {
        String aboutToDeployTemplateName = env.getProperty("bootrest.templateName").replaceAll("\\s", "");
        Set<ProcessGroupEntity> processGroups = pgfe.getProcessGroupFlow().getFlow().getProcessGroups();

        for (ProcessGroupEntity pge : processGroups) {
            if (aboutToDeployTemplateName.equalsIgnoreCase(pge.getComponent().getName().replaceAll("\\s", ""))) {
                return pge;

            }
        }
        return null;
    }

    /**
     * This is the method which is used to get the working process Group ID
     *
     * @param pgfe
     * @return
     */
    @SuppressWarnings("unused")
    private ProcessGroupEntity getAboutToDeployProcessGroupId(ProcessGroupFlowEntity pgfe, String templateName) {
        String aboutToDeployTemplateName = templateName.replaceAll("\\s", "");
        Set<ProcessGroupEntity> processGroups = pgfe.getProcessGroupFlow().getFlow().getProcessGroups();

        for (ProcessGroupEntity pge : processGroups) {
            if (aboutToDeployTemplateName.equalsIgnoreCase(pge.getComponent().getName().replaceAll("\\s", ""))) {
                return pge;

            }
        }
        return null;
    }




}
