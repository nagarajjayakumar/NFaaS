package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ControllerService {


    private static final Logger logger = LoggerFactory.getLogger(ControllerService.class);

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
    ControllerService(Environment env) {

        logger.info("Intialized ProcessGroupFlow !!! ");
        this.env = env;
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
    }

    /**
     * Enable or disbale the contoller service state
     *
     * @param controllerServiceEntity
     * @param state
     */
    public void changeControllServiceState(ControllerServiceEntity controllerServiceEntity, String state) {
        String contServid = controllerServiceEntity.getId();

        ControllerServiceEntity controllerServiceEntityReq = new ControllerServiceEntity();
        controllerServiceEntityReq.setId(contServid);
        copyRevision(controllerServiceEntity, controllerServiceEntityReq);

        copyControllerServiceEntityState(controllerServiceEntity, controllerServiceEntityReq);

        controllerServiceEntityReq.getComponent().setState(state);

        commonService.updateLastModified(controllerServiceEntityReq);

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/controller-services/" + contServid + "/";

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders headers = security.getAuthorizationHeader();
        HttpEntity<ControllerServiceEntity> requestEntity = new HttpEntity<>(controllerServiceEntityReq, headers);

        try {
            HttpEntity<ControllerServiceEntity> response = restTemplate.exchange(uri, HttpMethod.PUT, requestEntity,
                    ControllerServiceEntity.class, params);

            ControllerServiceEntity resp = response.getBody();

            logger.debug(resp.toString());
        } catch (HttpClientErrorException clientErrorException) {
            if (clientErrorException.getRawStatusCode() != HttpStatus.CONFLICT.value())
                throw new RuntimeException(clientErrorException.getMessage());
            else
                logger.error("Controller Services is in invalid state.. Please validate --> "
                        + controllerServiceEntity.getComponent().getName());
        }
    }

    /**
     * Method is to Stop the referencing components of the Controller Services.
     *
     * @param controllerServiceEntity
     * @param state
     */
    public void disableControllerServiceUsingRef(ControllerServiceEntity controllerServiceEntity, String state) {

        UpdateControllerServiceReferenceRequestEntity updateContServRefReq = new UpdateControllerServiceReferenceRequestEntity();

        updateContServRefReq.setId(controllerServiceEntity.getId());
        updateContServRefReq.setState(state);

        commonService.updateLastModified(controllerServiceEntity);

        Map<String, RevisionDTO> refCompRevisions = new HashMap<>();

        updateContServRefReq.setReferencingComponentRevisions(refCompRevisions);

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/controller-services/"
                + controllerServiceEntity.getId() + "/references/";

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders headers = security.getAuthorizationHeader();
        HttpEntity<UpdateControllerServiceReferenceRequestEntity> requestEntity = new HttpEntity<>(updateContServRefReq,
                headers);

        HttpEntity<ControllerServiceReferencingComponentsEntity> response = restTemplate.exchange(uri, HttpMethod.PUT,
                requestEntity, ControllerServiceReferencingComponentsEntity.class, params);

        ControllerServiceReferencingComponentsEntity resp = response.getBody();

        logger.debug(resp.toString());

    }

    /**
     * Method is to Stop the referencing components of the Controller Services.
     *
     * @param controllerServiceEntity
     * @param state
     */
    public void stopReferencingComponents(ControllerServiceEntity controllerServiceEntity, String state) {

        String contServid = controllerServiceEntity.getId();
        Set<ControllerServiceReferencingComponentEntity> referencingComponents = controllerServiceEntity.getComponent()
                .getReferencingComponents();

        UpdateControllerServiceReferenceRequestEntity updateContServRefReq = new UpdateControllerServiceReferenceRequestEntity();

        updateContServRefReq.setId(contServid);
        updateContServRefReq.setState(state);

        Map<String, RevisionDTO> refCompRevisions = new HashMap<>();

        for (ControllerServiceReferencingComponentEntity csRefComp : referencingComponents) {
            refCompRevisions.put(csRefComp.getId(), commonService.updateLastModified(csRefComp));
        }

        updateContServRefReq.setReferencingComponentRevisions(refCompRevisions);

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/controller-services/" + contServid
                + "/references/";

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders headers = security.getAuthorizationHeader();
        HttpEntity<UpdateControllerServiceReferenceRequestEntity> requestEntity = new HttpEntity<>(updateContServRefReq,
                headers);

        HttpEntity<ControllerServiceReferencingComponentsEntity> response = restTemplate.exchange(uri, HttpMethod.PUT,
                requestEntity, ControllerServiceReferencingComponentsEntity.class, params);

        ControllerServiceReferencingComponentsEntity resp = response.getBody();

        logger.debug(resp.toString());

    }

    /**
     * Delete teh controller Service
     *
     * @param controllerServiceEntity
     * @param state
     */
    public ControllerServiceEntity deleteControllerService(ControllerServiceEntity controllerServiceEntity,
                                                            String state) {

        logger.info("Delete Controller Service Entity Starts --> " + controllerServiceEntity.getComponent().getName());
        String contServid = controllerServiceEntity.getId();

        // https://"+nifiServerHostnameAndPort+"/nifi-api/controller-services/b369d993-48ae-4c0e-5ddc-ac8b8f316c4b?version=2&clientId=deaebc77-015b-1000-31ea-162516e98255
        String version = String.valueOf(commonService.getClientIdAndVersion(controllerServiceEntity).getVersion());
        String clientId = String.valueOf(commonService.getClientIdAndVersion(controllerServiceEntity).getClientId());

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/controller-services/" + contServid
                + "?version=" + version + "&clientId=" + clientId;

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        HttpEntity<ControllerServiceEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity,
                ControllerServiceEntity.class, params);

        ControllerServiceEntity resp = response.getBody();

        logger.debug(resp.toString());
        logger.info("Delete Controller Service Entity Ends --> " + controllerServiceEntity.getComponent().getName());
        return resp;

    }


    /**
     * get the latest controller services status
     *
     * @param controllerServiceEntity
     * @return
     */
    public ControllerServiceEntity getLatestControllerServiceEntity(ControllerServiceEntity controllerServiceEntity) {

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/controller-services/"
                + controllerServiceEntity.getId() + "/";
        HttpEntity<ControllerServiceEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                ControllerServiceEntity.class, params);
        return response.getBody();
    }

    private void copyRevision(ComponentEntity controllerServiceEntitySrc, ComponentEntity controllerServiceEntityDest) {
        RevisionDTO revision = new RevisionDTO();
        revision.setClientId(controllerServiceEntitySrc.getRevision().getClientId());
        revision.setVersion(controllerServiceEntitySrc.getRevision().getVersion());
        revision.setLastModifier(controllerServiceEntitySrc.getRevision().getLastModifier());

        controllerServiceEntityDest.setRevision(revision);

    }

    private void copyControllerServiceEntityState(ControllerServiceEntity controllerServiceEntitySrc,
                                                  ControllerServiceEntity controllerServiceEntityDest) {
        ControllerServiceDTO component = new ControllerServiceDTO();
        component.setId(controllerServiceEntitySrc.getId());
        controllerServiceEntityDest.setComponent(component);
    }


}
