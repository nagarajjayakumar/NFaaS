package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupsEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class ProcessGroups {

    private static final Logger logger = LoggerFactory.getLogger(ProcessGroups.class);

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
    ProcessGroups(Environment env) {
        this.env = env;
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
    }

    @CrossOrigin
    @PreAuthorize("#oauth2.hasScope('read')")
    @RequestMapping(value = "/load/process/all", produces = "application/json")
    public @ResponseBody
    ProcessGroupFlowEntity loadAllProcessGroups() {
        restTemplate = security.ignoreCertAndHostVerification(restTemplate);
        logger.info("bootrest.customproperty " + env.getProperty("bootrest.customproperty"));
        ProcessGroupFlowEntity pge = processGroupFlow.getRootProcessGroupFlowEntity();
        logger.info(pge.toString());
        return pge;
    }

    @CrossOrigin
    @PreAuthorize("#oauth2.hasScope('read')")
    @RequestMapping(value = "/processor-groups/deploy/{pgId}", produces = "application/json")
    public @ResponseBody
    ProcessGroupEntity deployProcessGroupByPgId(@PathVariable("pgId") String pgId) {
        restTemplate = security.ignoreCertAndHostVerification(restTemplate);
        ProcessGroupEntity pge = getLatestProcessGroupEntity(pgId);
        deployAndStartProcessGroup(pge);
        logger.info(pge.toString());
        return pge;
    }


    /**
     * this is the method to get the Latest process Group Entity
     *
     * @param pgId
     * @return
     */
    private ProcessGroupEntity getLatestProcessGroupEntity(String pgId) {
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
    private ProcessGroupEntity deleteProcessGroup(ProcessGroupEntity pge, String state) {

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

    /**
     * This is the method to get all the processGroupEntity from the pgfe
     *
     * @param pgfe
     * @param processGroupsFromTemplate
     * @return
     */
    public Set<ProcessGroupEntity> getProcessGroupEntityForUndeploy(ProcessGroupFlowEntity pgfe,
                                                                    Set<ProcessGroupDTO> processGroupsFromTemplate) {

        Set<ProcessGroupEntity> resultProcessGroups = new LinkedHashSet<>();
        Set<ProcessGroupEntity> allProcessGroups = pgfe.getProcessGroupFlow().getFlow().getProcessGroups();

        Set<String> processGroupNameFromTemplate = getAllProcessGroupNameFromTemplate(processGroupsFromTemplate);

        for (ProcessGroupEntity pge : allProcessGroups) {
            if (processGroupNameFromTemplate.contains(pge.getComponent().getName())) {
                resultProcessGroups.add(pge);
            }

        }
        return resultProcessGroups;
    }


    private Set<String> getAllProcessGroupNameFromTemplate(Set<ProcessGroupDTO> processGroupsFromTemplate) {
        Set<String> processGroupNameFromTemplate = new LinkedHashSet<>();

        for (ProcessGroupDTO processGroupDTO : processGroupsFromTemplate) {
            processGroupNameFromTemplate.add(processGroupDTO.getName());
        }

        return processGroupNameFromTemplate;
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
