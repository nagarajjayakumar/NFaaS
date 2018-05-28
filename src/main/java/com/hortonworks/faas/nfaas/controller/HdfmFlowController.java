package com.hortonworks.faas.nfaas.controller;

import com.hortonworks.faas.nfaas.config.EntityState;
import org.apache.commons.io.IOUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.nifi.web.api.dto.*;
import org.apache.nifi.web.api.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.TimeUnit;

@RestController
public class HdfmFlowController {

    private static final Logger logger = LoggerFactory.getLogger(HdfmFlowController.class);

    Environment env;

    private int WAIT_IN_SEC = 10;

    private String nifiServerHostnameAndPort = "localhost:9090";

    private String templateFileLocation = "classpath:Hello_NiFi_Web_Service.xml";

    private String templateFileURI = "https://cwiki.apache.org/confluence/download/attachments/57904847/Hello_NiFi_Web_Service.xml?version=1&modificationDate=1449369797000&api=v2";

    private String templateFileLoadFrom = "FILE";

    private boolean deleteQueueContent = false;

    private boolean undeployOnly = false;

    private String nifiUsername = "admin";
    private String nifiPassword = "BadPass#1";

    private String trasnsportMode = "http";

    private boolean nifiSecuredCluster = false;
    private boolean enableRPG = false;

    // "Authorization",
    // "Bearer
    // eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJIMjI4MzQ4IiwiaXNzIjoiTGRhcFByb3ZpZGVyIiwiYXVkIjoiTGRhcFByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiSDIyODM0OCIsImtpZCI6MSwiZXhwIjoxNDk0NDAzODM1LCJpYXQiOjE0OTQzNjA2MzV9.ztHHOr4uAnxa8Yx2qv5QV2b8grBxjHDx6vkUfYw00zQ"
    private final String authorizationHeaderKey = "Authorization";
    private final String authorizationHeaderValue = "Bearer ";

    @Autowired
    HdfmFlowController(Environment env) {
        this.env = env;
        this.WAIT_IN_SEC = Integer.parseInt(env.getProperty("nifi.component.status.wait.sec"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
        this.templateFileLocation = env.getProperty("bootrest.templateFileLocation");
        this.templateFileURI = env.getProperty("bootrest.templateFileURI");
        this.templateFileLoadFrom = env.getProperty("bootrest.templateFileLoadFrom");
        this.deleteQueueContent = Boolean.parseBoolean(env.getProperty("bootrest.deleteQueueContent"));
        this.undeployOnly = Boolean.parseBoolean(env.getProperty("bootrest.undeployOnly"));
        this.nifiUsername = env.getProperty("bootrest.nifiUsername");
        this.nifiPassword = env.getProperty("bootrest.nifiPassword");
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");

        this.enableRPG = Boolean.parseBoolean(env.getProperty("bootrest.enableRPG"));
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
    }

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    private ResourceLoader resourceLoader;

    @CrossOrigin
    @PreAuthorize("#oauth2.hasScope('read')")
    @RequestMapping(value = "/flow/deploy", produces = "application/json")
    public @ResponseBody
    ProcessGroupFlowEntity deployFlows() {
        restTemplate = ignoreCertAndHostVerification(restTemplate);
        logger.info("bootrest.templateName " + env.getProperty("bootrest.templateName"));
        logger.info("Get Root Process Group ID ");

        // Return the recent Process Group Flow Entity ....
        ProcessGroupFlowEntity pgfe = getRootProcessGroupFlowEntity();

        String rootPgId = pgfe.getProcessGroupFlow().getId();
        logger.debug("Root PG ID is :: " + rootPgId);

        undeployFLow(pgfe, rootPgId);

        if (!undeployOnly) {
            logger.info("Deploy the Process Group Started ");
            ProcessGroupEntity pge = deployProcessGroupByPgId(rootPgId);
            logger.info("Deploy the  Process Group Ended " + pge.toString());
        }
        logger.info("#########################################################################################");

        // Return the recent Process Group Flow Entity ....
        pgfe = getRootProcessGroupFlowEntity();
        logger.info(pgfe.toString());
        return pgfe;
    }

    /**
     * This is the method which is used to undeploy the FLOW
     *
     * @param pgfe
     */
    private void undeployFLow(ProcessGroupFlowEntity pgfe, String rootPgId) {

        logger.info("Read the template from .." + templateFileLocation);
        TemplateDTO template = readTemplateUsingLoadFromParam();

        logger.info("#########################################################################################");
        logger.info("Undeploy the requested Flow BEGINS ");
        stopAllEntitySpecifiedInTemplate(pgfe, template);
        deleteRootProcessGroupQueueContentIfAny(rootPgId);
        deleteAllEntitySpecifiedInTemplate(pgfe, template);

        logger.info("Undeploy the requested Flow ENDS ");
        logger.info("#########################################################################################");

    }

    /**
     * Stop all the component specified in the Input template
     *
     * @param pgfe
     * @param template
     */
    private void stopAllEntitySpecifiedInTemplate(ProcessGroupFlowEntity pgfe, TemplateDTO template) {

        Set<ProcessGroupDTO> processGroupsFromTemplate = template.getSnippet().getProcessGroups();
        Set<PortDTO> inputPortsFromTemplate = template.getSnippet().getInputPorts();
        Set<PortDTO> outputPortsFromTemplate = template.getSnippet().getOutputPorts();
        Set<ProcessorDTO> processorsFromTemplate = template.getSnippet().getProcessors();
        // Set<RemoteProcessGroupDTO> remoteProcessGroupsFromTemplate =
        // template.getSnippet().getRemoteProcessGroups();

        logger.info("About to stop all the entity Specified in the File Begin ...");
        Set<ProcessGroupEntity> processGroups = getProcessGroupEntityForUndeploy(pgfe, processGroupsFromTemplate);
        Set<PortEntity> inputPorts = getInputPortsEntityForUndeploy(pgfe, inputPortsFromTemplate);
        Set<PortEntity> outputPorts = getOutputPortsEntityForUndeploy(pgfe, outputPortsFromTemplate);
        Set<ProcessorEntity> processors = getProcessorEntityForUndeploy(pgfe, processorsFromTemplate);
        // Set<RemoteProcessGroupEntity> remoteProcessGroups =
        // getRemoteProcessGroupEntityForUndeploy(pgfe,
        // remoteProcessGroupsFromTemplate);

        if (processGroups.isEmpty()) {
            logger.info("Skiping 1 :: No Process Group Found for the Template Name  ::"
                    + env.getProperty("bootrest.templateName"));
        }
        ControllerServicesEntity cse = null;
        for (ProcessGroupEntity processorGroup : processGroups) {
            logger.info("Step 1 :: stopAllEntitySpecifiedInTemplate PG starts --> "
                    + processorGroup.getComponent().getName());
            cse = getAllControllerServicesByProcessGroup(processorGroup.getId());

            logger.info("Step 1.1 :: disable the Controller Services For the Process Group Started "
                    + processorGroup.getId());
            disableAllControllerServices(cse);
            logger.info("Step 1.1 :: disable the Controller Services For the Process Group Ended " + cse.toString());

            logger.info("Step 1.2 :: Stop all the processor For the Process Group Started " + processorGroup.getId());
            stopAllProcessors(processorGroup.getId());
            logger.info("Step 1.2 :: Stop all the processor For the Process Group Started " + processorGroup.getId());

            logger.info(
                    "Step 1.3 :: disable all the Remote PROCESSOR Group For the Process Group Started " + processorGroup.getId());
            disableRemoteProcessGroup(processorGroup.getId());
            logger.info(
                    "Step 1.3 :: disable all the Remote PROCESSOR Group For the Process Group Started " + processorGroup.getId());

            logger.info("Step 1 :: stopAllEntitySpecifiedInTemplate PG Ends --> "
                    + processorGroup.getComponent().getName());
        }

        if (inputPorts.isEmpty()) {
            logger.info("Skiping 2 :: No Input Ports Found for the Template Name  ::"
                    + env.getProperty("bootrest.templateName"));
        }
        PortEntity ippe = null;
        for (PortEntity ipPortEntity : inputPorts) {
            logger.info("Step 2 :: stopAllEntitySpecifiedInTemplate inputports starts --> "
                    + ipPortEntity.getComponent().getName());
            ippe = stopInputPortEntity(ipPortEntity);
            logger.info("Step 2 :: stopAllEntitySpecifiedInTemplate inputports ends   --> "
                    + ipPortEntity.getComponent().getName() + ippe.toString());
        }

        if (outputPorts.isEmpty()) {
            logger.info("Skiping 3 :: No Output Ports Found for the Template Name  ::"
                    + env.getProperty("bootrest.templateName"));
        }
        PortEntity oppe = null;
        for (PortEntity opPortEntity : outputPorts) {
            logger.info("Step 3 :: stopAllEntitySpecifiedInTemplate outputPorts starts --> "
                    + opPortEntity.getComponent().getName());
            oppe = stopOutputPortEntity(opPortEntity);
            logger.info("Step 3 :: stopAllEntitySpecifiedInTemplate outputPorts ends   --> "
                    + opPortEntity.getComponent().getName() + oppe.toString());
        }

        ProcessorEntity procent = null;
        if (processors.isEmpty()) {
            logger.info("Skiping 4 :: No Processor Found for the Template Name  ::"
                    + env.getProperty("bootrest.templateName"));
        }
        for (ProcessorEntity processor : processors) {
            logger.info("Step 4 :: stopAllEntitySpecifiedInTemplate processor starts --> "
                    + processor.getComponent().getName());
            procent = stopProcessorEntity(processor);
            logger.info("Step 4 :: stopAllEntitySpecifiedInTemplate processor ends   --> "
                    + processor.getComponent().getName() + procent.toString());
        }
        logger.info("About to stop all the entity Specified in the File ENDS ...");
    }

    private void deleteAllEntitySpecifiedInTemplate(ProcessGroupFlowEntity pgfe, TemplateDTO template) {

        Set<ProcessGroupDTO> processGroupsFromTemplate = template.getSnippet().getProcessGroups();
        Set<PortDTO> inputPortsFromTemplate = template.getSnippet().getInputPorts();
        Set<PortDTO> outputPortsFromTemplate = template.getSnippet().getOutputPorts();
        Set<ProcessorDTO> processorsFromTemplate = template.getSnippet().getProcessors();

        logger.info("About to delete all the entity Specified in the File Begin ...");
        Set<ProcessGroupEntity> processGroups = getProcessGroupEntityForUndeploy(pgfe, processGroupsFromTemplate);
        Set<PortEntity> inputPorts = getInputPortsEntityForUndeploy(pgfe, inputPortsFromTemplate);
        Set<PortEntity> outputPorts = getOutputPortsEntityForUndeploy(pgfe, outputPortsFromTemplate);
        Set<ProcessorEntity> processors = getProcessorEntityForUndeploy(pgfe, processorsFromTemplate);

        if (inputPorts.isEmpty()) {
            logger.info("Skiping 5 :: No Input Ports Found for the Template Name  ::"
                    + env.getProperty("bootrest.templateName"));
        }
        for (PortEntity ipPortEntity : inputPorts) {
            logger.info("Step 5 :: deleteAllEntitySpecifiedInTemplate inputports starts --> "
                    + ipPortEntity.getComponent().getName());
            deleteInputPortEntity(ipPortEntity);
            logger.info("Step 5 :: deleteAllEntitySpecifiedInTemplate inputports ends   --> "
                    + ipPortEntity.getComponent().getName());
        }

        if (outputPorts.isEmpty()) {
            logger.info("Skiping 6 :: No Output Ports Found for the Template Name  ::"
                    + env.getProperty("bootrest.templateName"));
        }
        for (PortEntity opPortEntity : outputPorts) {
            logger.info("Step 6 :: deleteAllEntitySpecifiedInTemplate outputPorts starts --> "
                    + opPortEntity.getComponent().getName());
            deleteOutputPortEntity(opPortEntity);
            logger.info("Step 6 :: deleteAllEntitySpecifiedInTemplate outputPorts ends   --> "
                    + opPortEntity.getComponent().getName());
        }

        if (processors.isEmpty()) {
            logger.info("Skiping 7 :: No Processor Found for the Template Name  ::"
                    + env.getProperty("bootrest.templateName"));
        }
        for (ProcessorEntity processor : processors) {
            logger.info("Step 7 :: deleteAllEntitySpecifiedInTemplate processor starts --> "
                    + processor.getComponent().getName());
            deleteProcessorEntity(processor);
            logger.info("Step 7 :: deleteAllEntitySpecifiedInTemplate processor ends   --> "
                    + processor.getComponent().getName());
        }

        if (processGroups.isEmpty()) {
            logger.info("Skiping 8 :: No Process Group Found for the Template Name  ::"
                    + env.getProperty("bootrest.templateName"));
        }
        ControllerServicesEntity cse = null;
        for (ProcessGroupEntity processorGroup : processGroups) {
            logger.info("Step 8 :: deleteAllEntitySpecifiedInTemplate PG starts --> "
                    + processorGroup.getComponent().getName());
            cse = getAllControllerServicesByProcessGroup(processorGroup.getId());
            if (null != cse) {
                logger.info("Step 8.1 :: delete all the Controller Services For the Process Group Started "
                        + processorGroup.getId());
                deleteAllControllerServices(cse);
                logger.info(
                        "Step 8.1 :: delete all the Controller Services For the Process Group Ended " + cse.toString());
            }

            logger.info("Step 8.2 :: delete all the remote processor group For the Process Group Started "
                    + processorGroup.getId());
            //deleteAllRemoteProcessGroup(processorGroup.getId());
            logger.info("Step 8.2 :: delete all the remote processor group For the Process Group Started "
                    + processorGroup.getId());

            logger.info("Step 8.3 :: delete all the processor For the Process Group Started " + processorGroup.getId());
            deleteAllProcessors(processorGroup.getId());
            logger.info("Step 8.3 :: delete all the processor For the Process Group Started " + processorGroup.getId());

            logger.info("Step 8 :: deleteAllEntitySpecifiedInTemplate PG Ends --> "
                    + processorGroup.getComponent().getName());
        }

        logger.info("About to delete all the entity Specified in the File ENDS ...");

    }



    @CrossOrigin
    @PreAuthorize("#oauth2.hasScope('read')")
    @RequestMapping(value = "/controller-services/undeploy/{pgId}", produces = "application/json")
    public @ResponseBody
    ControllerServicesEntity unDeployControllerServicesForProcessGroup(
            @PathVariable("pgId") String pgId) {
        restTemplate = ignoreCertAndHostVerification(restTemplate);
        logger.info("bootrest.customproperty " + env.getProperty("bootrest.customproperty"));
        ControllerServicesEntity cse = getAllControllerServicesByProcessGroup(pgId);

        stopAndUnDeployControllerServices(cse);
        logger.info(cse.toString());
        cse = getAllControllerServicesByProcessGroup(pgId);
        return cse;
    }

    @CrossOrigin
    @PreAuthorize("#oauth2.hasScope('read')")
    @RequestMapping(value = "/processor-groups/undeploy/{pgId}", produces = "application/json")
    public @ResponseBody
    ProcessGroupFlowEntity unDeployProcessGroupByPgId(@PathVariable("pgId") String pgId) {
        restTemplate = ignoreCertAndHostVerification(restTemplate);
        logger.info("bootrest.customproperty " + env.getProperty("bootrest.customproperty"));

        // https://"+nifiServerHostnameAndPort+"/nifi-api/flow/process-groups/a57d7d2a-86bd-4b43-113d-e0abfb83bd9b
        ProcessGroupFlowEntity pgfe = getLatestProcessGroupFlowEntity(pgId);

        Set<ProcessGroupEntity> processGroups = pgfe.getProcessGroupFlow().getFlow().getProcessGroups();

        ControllerServicesEntity cse = null;
        for (ProcessGroupEntity processorGroup : processGroups) {
            logger.info("Step 1 :: stopAllEntitySpecifiedInTemplate PG starts --> "
                    + processorGroup.getComponent().getName());
            cse = getAllControllerServicesByProcessGroup(processorGroup.getId());

            logger.info("Step 1.1 :: disable the Controller Services For the Process Group Started "
                    + processorGroup.getId());
            disableAllControllerServices(cse);
            logger.info("Step 1.1 :: disable the Controller Services For the Process Group Ended " + cse.toString());

            logger.info("Step 1.2 :: Stop all the processor For the Process Group Started " + processorGroup.getId());
            stopAllProcessors(processorGroup.getId());
            logger.info("Step 1.2 :: Stop all the processor For the Process Group Started " + processorGroup.getId());

            logger.info(
                    "Step 1.3 :: disable all the processor For the Process Group Started " + processorGroup.getId());
            disableRemoteProcessGroup(processorGroup.getId());
            logger.info(
                    "Step 1.3 :: disable all the processor For the Process Group Started " + processorGroup.getId());

            logger.info("Step 1 :: stopAllEntitySpecifiedInTemplate PG Ends --> "
                    + processorGroup.getComponent().getName());
        }
        pgfe = getLatestProcessGroupFlowEntity(pgId);
        processGroups = pgfe.getProcessGroupFlow().getFlow().getProcessGroups();

        stopAndUnDeployProcessGroup(pgfe, pgId);

//		pgfe = getLatestProcessGroupFlowEntity(pgId);
//		processGroups = pgfe.getProcessGroupFlow().getFlow().getProcessGroups();
//
//		for (ProcessGroupEntity processorGroup : processGroups) {
//			logger.info("Step 8 :: deleteAllEntitySpecifiedInTemplate PG starts --> "
//					+ processorGroup.getComponent().getName());
//			cse = getAllControllerServicesByProcessGroup(processorGroup.getId());
//			if (null != cse) {
//				logger.info("Step 8.1 :: delete all the Controller Services For the Process Group Started "
//						+ processorGroup.getId());
//				deleteAllControllerServices(cse);
//				logger.info(
//						"Step 8.1 :: delete all the Controller Services For the Process Group Ended " + cse.toString());
//			}
//
//			logger.info("Step 8.2 :: delete all the remote processor group For the Process Group Started "
//					+ processorGroup.getId());
//			// deleteAllRemoteProcessGroup(processorGroup.getId());
//			logger.info("Step 8.2 :: delete all the remote processor group For the Process Group Started "
//					+ processorGroup.getId());
//
//			logger.info("Step 8.3 :: delete all the processor For the Process Group Started " + processorGroup.getId());
//			deleteAllProcessors(processorGroup.getId());
//			logger.info("Step 8.3 :: delete all the processor For the Process Group Started " + processorGroup.getId());
//
//			logger.info("Step 8 :: deleteAllEntitySpecifiedInTemplate PG Ends --> "
//					+ processorGroup.getComponent().getName());
//		}
//		stopAndUnDeployProcessGroup(pgfe, pgId);
        return pgfe;
    }







    /**
     * This is the method to get all the input port entity from the pgfe
     *
     * @param pgfe
     * @param inputPortFromTemplate
     * @return
     */
    private Set<PortEntity> getInputPortsEntityForUndeploy(ProcessGroupFlowEntity pgfe,
                                                           Set<PortDTO> inputPortFromTemplate) {

        Set<PortEntity> resultInputPorts = new LinkedHashSet<>();
        Set<PortEntity> allInputPorts = pgfe.getProcessGroupFlow().getFlow().getInputPorts();

        Set<String> inputPortsNameFromTemplate = getAllInputPortNameFromTemplate(inputPortFromTemplate);

        for (PortEntity pe : allInputPorts) {
            if (inputPortsNameFromTemplate.contains(pe.getComponent().getName())) {
                resultInputPorts.add(pe);
            }

        }
        return resultInputPorts;
    }

    /**
     * This is the method to get all the Output port entity from the pgfe
     *
     * @param pgfe
     * @param outputPortFromTemplate
     * @return
     */
    private Set<PortEntity> getOutputPortsEntityForUndeploy(ProcessGroupFlowEntity pgfe,
                                                            Set<PortDTO> outputPortFromTemplate) {

        Set<PortEntity> resultOutputPorts = new LinkedHashSet<>();
        Set<PortEntity> allOutputPorts = pgfe.getProcessGroupFlow().getFlow().getOutputPorts();

        Set<String> outputPortsNameFromTemplate = getAllOutputPortNameFromTemplate(outputPortFromTemplate);

        for (PortEntity pe : allOutputPorts) {
            if (outputPortsNameFromTemplate.contains(pe.getComponent().getName())) {
                resultOutputPorts.add(pe);
            }

        }
        return resultOutputPorts;
    }

    /**
     * This is the method to get all the Processor entity from the pgfe
     *
     * @param pgfe
     * @param processorsFromTemplate
     * @return
     */
    private Set<ProcessorEntity> getProcessorEntityForUndeploy(ProcessGroupFlowEntity pgfe,
                                                               Set<ProcessorDTO> processorsFromTemplate) {

        Set<ProcessorEntity> resultProcessors = new LinkedHashSet<>();
        Set<ProcessorEntity> allProcessGroups = pgfe.getProcessGroupFlow().getFlow().getProcessors();

        Set<String> processorNameFromTemplate = getAllProcessorsNameFromTemplate(processorsFromTemplate);

        for (ProcessorEntity pe : allProcessGroups) {
            if (processorNameFromTemplate.contains(pe.getComponent().getName())) {
                resultProcessors.add(pe);
            }

        }
        return resultProcessors;
    }

    /**
     * This is the method which is used to get all the remote process group from
     * the Pgfe
     *
     * @param pgfe
     * @param remoteProcessGroupsFromTemplate
     * @return
     */
    @SuppressWarnings("unused")
    private Set<RemoteProcessGroupEntity> getRemoteProcessGroupEntityForUndeploy(ProcessGroupFlowEntity pgfe,
                                                                                 Set<RemoteProcessGroupDTO> remoteProcessGroupsFromTemplate) {

        Set<RemoteProcessGroupEntity> resultRemotePG = new LinkedHashSet<>();
        Set<RemoteProcessGroupEntity> allRemoteProcessGroups = pgfe.getProcessGroupFlow().getFlow()
                .getRemoteProcessGroups();

        Set<String> remoteProcessorGroupNameFromTemplate = getAllRemoteProcessorGroupNameFromTemplate(
                remoteProcessGroupsFromTemplate);

        for (RemoteProcessGroupEntity rpge : allRemoteProcessGroups) {
            if (remoteProcessorGroupNameFromTemplate.contains(rpge.getComponent().getName())) {
                resultRemotePG.add(rpge);
            }

        }
        return resultRemotePG;
    }

    /**
     * Method is used to instantiate the template and deploy and Start the
     * components.
     *
     * @param processGroupEntity
     */
    private FlowEntity deployAndStartProcessGroup(ProcessGroupEntity processGroupEntity) {

        FlowEntity flowEntity = createTemplateInstanceByTemplateId(processGroupEntity);
        Set<ProcessGroupEntity> processGroups = flowEntity.getFlow().getProcessGroups();

        Set<PortEntity> inputPorts = flowEntity.getFlow().getInputPorts();
        Set<PortEntity> outputPorts = flowEntity.getFlow().getOutputPorts();
        Set<ProcessorEntity> processors = flowEntity.getFlow().getProcessors();
        logger.info(flowEntity.toString());

        PortEntity ippe = null;
        for (PortEntity ipPortEntity : inputPorts) {
            logger.info("deployAndStartProcessGroup inputports starts --> " + ipPortEntity.getComponent().getName());
            ippe = startInputPortEntity(ipPortEntity);
            logger.info("deployAndStartProcessGroup inputports ends   --> " + ipPortEntity.getComponent().getName()
                    + ippe.toString());
        }

        PortEntity oppe = null;
        for (PortEntity opPortEntity : outputPorts) {
            logger.info("deployAndStartProcessGroup outputPorts starts --> " + opPortEntity.getComponent().getName());
            oppe = startOutputPortEntity(opPortEntity);
            logger.info("deployAndStartProcessGroup outputPorts ends   --> " + opPortEntity.getComponent().getName()
                    + oppe.toString());
        }

        ProcessorEntity procent = null;
        for (ProcessorEntity processor : processors) {
            logger.info("deployAndStartProcessGroup processor starts --> " + processor.getComponent().getName());
            procent = startProcessorEntity(processor);
            logger.info("deployAndStartProcessGroup processor ends   --> " + processor.getComponent().getName()
                    + procent.toString());
        }

        ControllerServicesEntity cse = null;
        for (ProcessGroupEntity processorGroup : processGroups) {
            logger.info("deployAndStartProcessGroup PG starts --> " + processorGroup.getComponent().getName());
            cse = getAllControllerServicesByProcessGroup(processorGroup.getId());
            enableAllControllerServices(cse);
            startAllProcessors(processorGroup);
            enableRemoteProcessGroup(processorGroup.getId());
            logger.info("deployAndStartProcessGroup PG Ends --> " + processorGroup.getComponent().getName());
        }

        return flowEntity;

    }

    /**
     * This is the method to stop and un-deploy the process group.
     *
     * @param pge
     */
    private void stopAndUnDeployProcessGroup(ProcessGroupFlowEntity processGroupFlowEntity, String pgId) {
        logger.info("stopAndUnDeployProcessGroup Starts for --> " + pgId);
        ProcessGroupFlowEntity pgfe = stopProcessGroupComponents(processGroupFlowEntity, null, pgId);
        logger.info(pgfe.toString());
        disableRemoteProcessGroup(pgId);
        ProcessGroupEntity pge = getLatestProcessGroupEntity(pgId);
        pge = deleteProcessGroup(pge);
        logger.info("stopAndUnDeployProcessGroup Ends for --> " + pgId);
    }

    /**
     * This is the method to stop the process group.
     *
     * @param pge
     */
    private void stopProcessGroup(ProcessGroupFlowEntity processGroupFlowEntity, String pgId) {
        logger.info("stopProcessGroup Starts for --> " + pgId);
        ProcessGroupFlowEntity pgfe = stopProcessGroupComponents(processGroupFlowEntity, null, pgId);
        logger.info(pgfe.toString());
        ProcessGroupEntity pge = getLatestProcessGroupEntity(pgId);
        logger.info("stopProcessGroup Ends for --> " + pge.getComponent().getName());
    }

    /**
     * This is the method to disable stop the process group.
     *
     * @param processGroupFlowEntity
     * @param pgId
     */
    private void disableRemoteProcessGroup(String pgId) {
        logger.debug("disableRemoteProcessGroup Starts for --> " + pgId);

        ProcessGroupFlowEntity pgfe = getLatestProcessGroupFlowEntity(pgId);
        Set<ProcessGroupEntity> processGroups = pgfe.getProcessGroupFlow().getFlow().getProcessGroups();

        for (ProcessGroupEntity processGroupEntity : processGroups) {
            if (processGroupEntity.getActiveRemotePortCount() > 0) {
                disableRemoteProcessGroup(processGroupEntity.getId());
            }
        }

        ProcessGroupEntity pge = getLatestProcessGroupEntity(pgId);
        RemoteProcessGroupsEntity remoteProcessGroupsEntity = getLatestRemoteProcessGroupsEntity(pgId);

        Set<RemoteProcessGroupEntity> remoteProcessGroups = remoteProcessGroupsEntity.getRemoteProcessGroups();

        if (remoteProcessGroups.isEmpty()) {
            logger.debug("No remote process group found for the PG " + pge.getComponent().getName());
            logger.debug("disableRemoteProcessGroup Ends for --> " + pge.getComponent().getName());
            return;
        }

        for (RemoteProcessGroupEntity rpge : remoteProcessGroups) {
            logger.info("disableRemoteProcessGroup Starts for --> " + pge.getComponent().getName());
            disableRemoteProcessGroupComponents(rpge);
            logger.info("disableRemoteProcessGroup Ends for --> " + pge.getComponent().getName());
        }
        pge = getLatestProcessGroupEntity(pgId);
        logger.debug("disableRemoteProcessGroup Ends for --> " + pge.getComponent().getName());

    }

    /**
     * This is the method to disable stop the process group.
     *
     * @param processGroupFlowEntity
     * @param pgId
     */
    private void enableRemoteProcessGroup(String pgId) {

        if (!enableRPG) {
            logger.error("DEMO :: enable remote process group skipping ");
            return;
        }

        ProcessGroupFlowEntity pgfe = getLatestProcessGroupFlowEntity(pgId);
        Set<ProcessGroupEntity> processGroups = pgfe.getProcessGroupFlow().getFlow().getProcessGroups();

        for (ProcessGroupEntity processGroupEntity : processGroups) {
            if (processGroupEntity.getInactiveRemotePortCount() > 0) {
                enableRemoteProcessGroup(processGroupEntity.getId());
            }
        }

        ProcessGroupEntity pge = getLatestProcessGroupEntity(pgId);
        RemoteProcessGroupsEntity remoteProcessGroupsEntity = getLatestRemoteProcessGroupsEntity(pgId);

        Set<RemoteProcessGroupEntity> remoteProcessGroups = remoteProcessGroupsEntity.getRemoteProcessGroups();

        if (remoteProcessGroups.isEmpty()) {
            logger.debug("No remote process group found for the PG " + pge.getComponent().getName());
            logger.debug("enableRemoteProcessGroup Ends for --> " + pge.getComponent().getName());
            return;
        }

        for (RemoteProcessGroupEntity rpge : remoteProcessGroups) {
            logger.info("enableRemoteProcessGroup Ends for --> " + pge.getComponent().getName());
            enableRemoteProcessGroupComponents(rpge);
            logger.info("enableRemoteProcessGroup Ends for --> " + pge.getComponent().getName());
        }
        pge = getLatestProcessGroupEntity(pgId);


    }

    /**
     * This is the method which is used to delete all the remote process group
     * for the PG
     *
     * @param id
     */
    @SuppressWarnings("unused")
    private void deleteAllRemoteProcessGroup(String pgId) {
        logger.info("deleteAllRemoteProcessGroup Starts for --> " + pgId);
        ProcessGroupEntity pge = getLatestProcessGroupEntity(pgId);
        RemoteProcessGroupsEntity remoteProcessGroupsEntity = getLatestRemoteProcessGroupsEntity(pgId);

        Set<RemoteProcessGroupEntity> remoteProcessGroups = remoteProcessGroupsEntity.getRemoteProcessGroups();

        if (remoteProcessGroups.isEmpty()) {
            logger.warn("No remote process group found for the PG " + pge.getComponent().getName());
            return;
        }

        for (RemoteProcessGroupEntity rpge : remoteProcessGroups) {
            deleteRemoteProcessGroupComponents(rpge);
        }
        pge = getLatestProcessGroupEntity(pgId);
        logger.info("deleteAllRemoteProcessGroup Ends for --> " + pge.toString());

    }

    /**
     * This is the method to stop and un-deploy the process group.
     *
     * @param pge
     */
    private void deleteProcessGroup(ProcessGroupFlowEntity processGroupFlowEntity, String pgId) {
        logger.info("deleteProcessGroup Starts for --> " + pgId);
        ProcessGroupEntity pge = getLatestProcessGroupEntity(pgId);
        pge = deleteProcessGroup(pge);
        logger.info("deleteProcessGroup Ends for --> " + pgId);
    }

    /**
     * Stop and Un deploy the controller Services.
     *
     * @param controllerServicesEntity
     */
    private void stopAndUnDeployControllerServices(ControllerServicesEntity controllerServicesEntity) {

        Set<ControllerServiceEntity> controllerServicesEntities = controllerServicesEntity.getControllerServices();

        ControllerServiceEntity cse = null;

        for (ControllerServiceEntity controllerServiceEntity : controllerServicesEntities) {
            logger.info("stopAndUnDeployControllerServices Starts for --> "
                    + controllerServiceEntity.getComponent().getName());
            cse = stopRefrencingComponents(controllerServiceEntity);
            cse = disableControllerService(cse);
            cse = deleteControllerService(cse);
            logger.info("stopAndUnDeployControllerServices Ends for --> "
                    + controllerServiceEntity.getComponent().getName());

        }

    }

    /**
     * Method is used to enable the controller services
     *
     * @param cse
     * @return
     */
    private void enableAllControllerServices(ControllerServicesEntity controllerServicesEntity) {
        Set<ControllerServiceEntity> controllerServicesEntities = controllerServicesEntity.getControllerServices();
        ControllerServiceEntity cse = null;
        for (ControllerServiceEntity controllerServiceEntity : controllerServicesEntities) {
            if (EntityState.INVALID.getState().equalsIgnoreCase(controllerServiceEntity.getComponent().getState())) {
                logger.error("Controller Services is in invalid state.. Please validate --> "
                        + controllerServiceEntity.getComponent().getName());
                continue;
            }
            logger.info("Controller Services Enable Starts --> " + controllerServiceEntity.getComponent().getName());
            cse = enableControllerService(controllerServiceEntity);
            logger.debug(cse.toString());
            logger.info("Controller Services Enable Ends   --> " + controllerServiceEntity.getComponent().getName());
        }
    }

    /**
     * Method is used to enable the controller services
     *
     * @param cse
     * @return
     */
    private void disableAllControllerServices(ControllerServicesEntity controllerServicesEntity) {
        Set<ControllerServiceEntity> controllerServicesEntities = controllerServicesEntity.getControllerServices();
        ControllerServiceEntity cse = null;
        for (ControllerServiceEntity controllerServiceEntity : controllerServicesEntities) {
            logger.info(
                    "disableAllControllerServices Starts for --> " + controllerServiceEntity.getComponent().getName());
            cse = stopRefrencingComponents(controllerServiceEntity);
            cse = disableControllerService(cse);
            logger.info(
                    "disableAllControllerServices Ends for --> " + controllerServiceEntity.getComponent().getName());
        }
    }

    /**
     * Method is used to enable the controller services
     *
     * @param cse
     * @return
     */
    private void deleteAllControllerServices(ControllerServicesEntity controllerServicesEntity) {
        Set<ControllerServiceEntity> controllerServicesEntities = controllerServicesEntity.getControllerServices();
        ControllerServiceEntity cse = null;
        for (ControllerServiceEntity controllerServiceEntity : controllerServicesEntities) {
            logger.info(
                    "deleteAllControllerServices Starts for --> " + controllerServiceEntity.getComponent().getName());
            cse = deleteControllerService(controllerServiceEntity);
            logger.info("deleteAllControllerServices Ends for --> " + controllerServiceEntity.getComponent().getName()
                    + cse.toString());
        }
    }

    /**
     * This is the method to start all the Processors
     *
     * @param processorGroup
     * @return
     */
    private ProcessGroupFlowEntity startAllProcessors(ProcessGroupEntity processorGroup) {

        logger.info("Process Group Starting Starts --> " + processorGroup.getComponent().getName());
        ProcessGroupFlowEntity processGroupFlowEntity = getLatestProcessGroupFlowEntity(processorGroup.getId());

        startProcessGroupComponents(processGroupFlowEntity, EntityState.RUNNING.getState());
        checkProcessGroupComponentStatus(processGroupFlowEntity, EntityState.RUNNING.getState(), processorGroup.getId());

        ProcessGroupFlowEntity pge = getLatestProcessGroupFlowEntity(
                processGroupFlowEntity.getProcessGroupFlow().getId());
        logger.info("Process Group Starting Ends  --> " + processorGroup.getComponent().getName());
        return pge;

    }

    /**
     * Method to stop all the process group components
     *
     * @param processGroupFlowEntity
     * @return
     */
    private ProcessGroupFlowEntity stopProcessGroupComponents(ProcessGroupFlowEntity processGroupFlowEntity,
                                                              ProcessGroupEntity processorGroup,
                                                              String pgId) {
        stopProcessGroupComponents(processGroupFlowEntity, EntityState.STOPPED.getState());
        checkProcessGroupComponentStatus(processGroupFlowEntity, EntityState.STOPPED.getState(), pgId);
        ProcessGroupFlowEntity pge = getLatestProcessGroupFlowEntity(
                processGroupFlowEntity.getProcessGroupFlow().getId());
        return pge;
    }

    /**
     * This is the method which is used to disable the remote process group
     * componets
     *
     * @param remoteProcessGroupEntity
     * @return
     */
    private RemoteProcessGroupEntity disableRemoteProcessGroupComponents(
            RemoteProcessGroupEntity remoteProcessGroupEntity) {
        disableRemoteProcessGroupComponents(remoteProcessGroupEntity, EntityState.TRANSMIT_FALSE.getState());

        checkRemoteProcessGroupComponentsStatus(remoteProcessGroupEntity, EntityState.TRANSMIT_FALSE.getState());
        RemoteProcessGroupEntity rpge = getLatestRemoteProcessGroupEntity(remoteProcessGroupEntity.getId());
        return rpge;

    }

    /**
     * Call the NIFI rest api to enable the process group
     *
     * @param remoteProcessGroupEntity
     * @param state
     */
    private RemoteProcessGroupEntity enableRemoteProcessGroupComponents(
            RemoteProcessGroupEntity remoteProcessGroupEntity) {
        enableRemoteProcessGroupComponents(remoteProcessGroupEntity, EntityState.TRANSMIT_TRUE.getState());

        checkRemoteProcessGroupComponentsStatus(remoteProcessGroupEntity, EntityState.TRANSMIT_TRUE.getState());
        RemoteProcessGroupEntity rpge = getLatestRemoteProcessGroupEntity(remoteProcessGroupEntity.getId());
        return rpge;

    }

    /**
     * Stop the referencing component of the controller services
     *
     * @param controllerServiceEntity
     * @return
     */
    private ControllerServiceEntity stopRefrencingComponents(ControllerServiceEntity controllerServiceEntity) {

        logger.info("Stopping Controller Refrence Component Starts --> "
                + controllerServiceEntity.getComponent().getName());
        stopReferencingComponents(controllerServiceEntity, EntityState.STOPPED.getState());

        checkReferenceComponentStatus(controllerServiceEntity, EntityState.STOPPED.getState());
        ControllerServiceEntity cse = getLatestControllerServiceEntity(controllerServiceEntity);
        logger.info(
                "Stopping Controller Refrence Component Ends --> " + controllerServiceEntity.getComponent().getName());
        return cse;
    }

    /**
     * Disable the controller Service.
     *
     * @param cse
     * @return
     */
    private ControllerServiceEntity disableControllerService(ControllerServiceEntity cse) {

        logger.info("Disable Controller Service Starts --> " + cse.getComponent().getName());
        disableControllerServiceUsingRef(cse, EntityState.DISABLED.getState());

        // No need to check the status now ..
        //checkControllerServiceStatus(cse, EntityState.DISABLED.getState());
        cse = getLatestControllerServiceEntity(cse);

        disableControllerService(cse, EntityState.DISABLED.getState());

        checkControllerServiceStatus(cse, EntityState.DISABLED.getState());
        logger.info("Disable Controller Service Ends --> " + cse.getComponent().getName());
        return getLatestControllerServiceEntity(cse);
    }

    /**
     * Disable the controller Service.
     *
     * @param cse
     * @return
     */
    private ControllerServiceEntity enableControllerService(ControllerServiceEntity cse) {

        logger.info("Enable Controller Service Starts --> " + cse.getComponent().getName());
        cse = getLatestControllerServiceEntity(cse);
        enableControllerService(cse, EntityState.ENABLED.getState());

        checkControllerServiceStatus(cse, EntityState.ENABLED.getState());
        logger.info("Enable Controller Service Ends --> " + cse.getComponent().getName());
        return getLatestControllerServiceEntity(cse);
    }

    /**
     * Delete teh controller Service
     *
     * @param controllerServiceEntity
     * @return
     */
    private ControllerServiceEntity deleteControllerService(ControllerServiceEntity controllerServiceEntity) {

        return deleteControllerService(controllerServiceEntity, EntityState.DELETE.getState());
    }

    /**
     * Delete the Process group
     *
     * @param pge
     * @return
     */
    private ProcessGroupEntity deleteProcessGroup(ProcessGroupEntity pge) {
        return deleteProcessGroup(pge, EntityState.DELETE.getState());
    }

    /**
     * Method is used to create the template Instance
     *
     * @param processGroupEntity
     * @return
     */
    private FlowEntity createTemplateInstanceByTemplateId(ProcessGroupEntity processGroupEntity) {

        String templateId = getTemplateId(processGroupEntity);

        if (null == templateId || templateId.isEmpty()) {
            throw new RuntimeException("Unable to upload the template ");
        }
        FlowEntity fe = createTemplateInstanceByTemplateId(processGroupEntity, templateId);
        return fe;
    }

    /**
     * Method is used to get the template ID
     *
     * @param processGroupEntity
     * @return
     */
    private String getTemplateId(ProcessGroupEntity processGroupEntity) {
        try {

            String templateId = checkTemplateExist();
            if (templateId != null)
                deleteTemplate(templateId);

            TemplateEntity templateEntity = uploadTemplate(processGroupEntity);

            return templateEntity.getTemplate().getId();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return "";
    }

    /**
     * This is the method to disable the controller service
     *
     * @param controllerServiceEntity
     * @param state
     */

    private void disableControllerService(ControllerServiceEntity controllerServiceEntity, String state) {
        changeControllServiceState(controllerServiceEntity, state);

    }

    /**
     * This is the method to Enable the controller service
     *
     * @param controllerServiceEntity
     * @param state
     */

    private void enableControllerService(ControllerServiceEntity controllerServiceEntity, String state) {
        changeControllServiceState(controllerServiceEntity, state);

    }

    /**
     * This is the method which is used to start the Port Entity ,,,,
     *
     * @param portEntity
     * @return
     */
    private PortEntity startInputPortEntity(PortEntity portEntity) {
        PortEntity pe = getLatestInputPortEntity(portEntity);
        pe = startOrStopInputPortEntity(pe, EntityState.RUNNING.getState());
        pe = checkInputPortStatus(pe, EntityState.RUNNING.getState());
        return pe;
    }

    /**
     * This is the method which is used to stop the Port Entity ,,,,
     *
     * @param portEntity
     * @return
     */
    private PortEntity stopInputPortEntity(PortEntity portEntity) {
        PortEntity pe = getLatestInputPortEntity(portEntity);
        pe = startOrStopInputPortEntity(pe, EntityState.STOPPED.getState());
        pe = checkInputPortStatus(pe, EntityState.STOPPED.getState());
        return pe;
    }

    /**
     * This is the method which is used to delete the Port Entity ,,,,
     *
     * @param portEntity
     * @return
     */
    private void deleteInputPortEntity(PortEntity portEntity) {
        PortEntity pe = getLatestInputPortEntity(portEntity);
        pe = deleteInputPortEntity(pe, EntityState.DELETE.getState());
        logger.info(pe.toString());
    }

    /**
     * This is the method which is used to start the Port Entity ,,,,
     *
     * @param portEntity
     * @return
     */
    private PortEntity startOutputPortEntity(PortEntity portEntity) {
        PortEntity pe = getLatestOutputPortEntity(portEntity);
        pe = startOrStopOutputPortEntity(pe, EntityState.RUNNING.getState());
        pe = checkOutputPortStatus(pe, EntityState.RUNNING.getState());
        return pe;
    }

    /**
     * This is the method which is used to stop the Port Entity ,,,,
     *
     * @param portEntity
     * @return
     */
    private PortEntity stopOutputPortEntity(PortEntity portEntity) {
        PortEntity pe = getLatestOutputPortEntity(portEntity);
        pe = startOrStopOutputPortEntity(pe, EntityState.STOPPED.getState());
        pe = checkOutputPortStatus(pe, EntityState.STOPPED.getState());
        return pe;
    }

    /**
     * This is the method which is used to stop the Port Entity ,,,,
     *
     * @param portEntity
     * @return
     */
    private void deleteOutputPortEntity(PortEntity portEntity) {
        PortEntity pe = getLatestOutputPortEntity(portEntity);
        pe = deleteOutputPortEntity(pe, EntityState.DELETE.getState());
        logger.info(pe.toString());
    }

    /**
     * This is the method which is used to Start the Processor Entity ,,,,
     *
     * @param processor
     * @return
     */
    private ProcessorEntity startProcessorEntity(ProcessorEntity processor) {
        ProcessorEntity pe = getLatestProcessorEntity(processor);
        if (EntityState.INVALID.getState().equalsIgnoreCase(pe.getStatus().getAggregateSnapshot().getRunStatus())) {
            logger.error("Procesor is in invalid state .. unable to start  " + pe.getComponent().getName());
            return pe;
        }
        pe = startOrStopProcessorEntity(pe, EntityState.RUNNING.getState());
        pe = checkProcessorEntityStatus(pe, EntityState.RUNNING.getState());
        return pe;
    }

    /**
     * This is the method which is used to stop the Processor Entity ,,,,
     *
     * @param processor
     * @return
     */
    private ProcessorEntity stopProcessorEntity(ProcessorEntity processor) {
        ProcessorEntity pe = getLatestProcessorEntity(processor);
        if (EntityState.INVALID.getState().equalsIgnoreCase(pe.getStatus().getAggregateSnapshot().getRunStatus())) {
            logger.error("Procesor is in invalid state unable to stop " + pe.getComponent().getName());
            return pe;
        }
        pe = startOrStopProcessorEntity(pe, EntityState.STOPPED.getState());
        pe = checkProcessorEntityStatus(pe, EntityState.STOPPED.getState());
        return pe;
    }

    /**
     * This is the method which is used to stop the Processor Entity ,,,,
     *
     * @param processor
     * @return
     */
    private void deleteProcessorEntity(ProcessorEntity processor) {
        ProcessorEntity pe = getLatestProcessorEntity(processor);
        pe = deleteProcessorEntity(pe, EntityState.DELETE.getState());
        logger.info(pe.toString());
    }

    /**
     * This is the method to Stop all processors for the process group
     *
     * @param pgId
     * @return
     */
    private ProcessGroupFlowEntity stopAllProcessors(String pgId) {
        ProcessGroupFlowEntity pgfe = getLatestProcessGroupFlowEntity(pgId);
        stopProcessGroup(pgfe, pgId);
        return pgfe;
    }

    /**
     * This is the method to Stop all processors for the process group
     *
     * @param pgId
     * @return
     */
    private ProcessGroupFlowEntity deleteAllProcessors(String pgId) {
        ProcessGroupFlowEntity pgfe = getLatestProcessGroupFlowEntity(pgId);
        deleteProcessGroup(pgfe, pgId);
        return pgfe;
    }


    /**
     * This is the method which is used to delete the remote process group
     * componets
     * http://localhost:8080/nifi-api/remote-process-groups/f2fe8ad1-015b-1000-64fd-caf013397f4a?version=6&clientId=f2fe58d9-015b-1000-f615-591b1d0de0c2
     *
     * @param rpge
     */
    private RemoteProcessGroupEntity deleteRemoteProcessGroupComponents(
            RemoteProcessGroupEntity remoteProcessGroupEntity) {

        logger.info(
                "Delete Remote Group Service Entity Starts --> " + remoteProcessGroupEntity.getComponent().getName());
        String rpgeId = remoteProcessGroupEntity.getId();

        // https://"+nifiServerHostnameAndPort+"/nifi-api/controller-services/b369d993-48ae-4c0e-5ddc-ac8b8f316c4b?version=2&clientId=deaebc77-015b-1000-31ea-162516e98255
        String version = String.valueOf(getClientIdAndVersion(remoteProcessGroupEntity).getVersion());
        String clientId = String.valueOf(getClientIdAndVersion(remoteProcessGroupEntity).getClientId());

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/remote-process-groups/" + rpgeId
                + "?version=" + version + "&clientId=" + clientId;

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        HttpEntity<RemoteProcessGroupEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity,
                RemoteProcessGroupEntity.class, params);

        RemoteProcessGroupEntity resp = response.getBody();

        logger.debug(resp.toString());
        logger.info("Delete Remote Group Entity Ends --> " + remoteProcessGroupEntity.getComponent().getName());
        return resp;

    }

    /**
     * Call the NIFI rest api to stop the process group
     *
     * @param processGroupFlowEntity
     * @param state
     */
    private void stopProcessGroupComponents(ProcessGroupFlowEntity processGroupFlowEntity, String state) {
        startOrStopProcessGroupComponents(processGroupFlowEntity, state);

    }

    /**
     * Call the NIFI rest api to stop the process group
     *
     * @param processGroupFlowEntity
     * @param state
     */
    private void startProcessGroupComponents(ProcessGroupFlowEntity processGroupFlowEntity, String state) {
        startOrStopProcessGroupComponents(processGroupFlowEntity, state);

    }

    /**
     * Call the NIFI rest api to disable the process group
     *
     * @param remoteProcessGroupEntity
     * @param state
     */
    private void disableRemoteProcessGroupComponents(RemoteProcessGroupEntity remoteProcessGroupEntity, String state) {
        enableOrDisableRemoteProcessGroupComponents(remoteProcessGroupEntity, state);
    }

    /**
     * Call the NIFI rest api to enable the process group
     *
     * @param remoteProcessGroupEntity
     * @param state
     */
    private void enableRemoteProcessGroupComponents(RemoteProcessGroupEntity remoteProcessGroupEntity, String state) {
        enableOrDisableRemoteProcessGroupComponents(remoteProcessGroupEntity, state);
    }





    /**
     * Call the NIFI rest api to start/stop the Processor
     * https://localhost:8080/nifi-api/processors/
     *
     * @param processor
     * @param state
     * @return
     */
    private ProcessorEntity startOrStopProcessorEntity(ProcessorEntity processor, String state) {

        String processorId = processor.getComponent().getId();

        ProcessorEntity processorEntityReq = new ProcessorEntity();
        ProcessorDTO component = new ProcessorDTO();
        processorEntityReq.setComponent(component);
        RevisionDTO revision = new RevisionDTO();

        BeanUtils.copyProperties(processor.getRevision(), revision);

        processorEntityReq.getComponent().setId(processorId);
        processorEntityReq.getComponent().setState(state);
        processorEntityReq.setRevision(revision);

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/processors/" + processorId + "/";

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders headers = getAuthorizationHeader();
        HttpEntity<ProcessorEntity> requestEntity = new HttpEntity<>(processorEntityReq, headers);

        HttpEntity<ProcessorEntity> response = restTemplate.exchange(uri, HttpMethod.PUT, requestEntity,
                ProcessorEntity.class, params);

        ProcessorEntity resp = response.getBody();

        logger.debug(resp.toString());

        return resp;
    }

    /**
     * This is the method which us used to enable or disable the remote process
     * Group components
     * http://localhost:8080/nifi-api/remote-process-groups/f2fe8ad1-015b-1000-64fd-caf013397f4a
     *
     * @param remoteProcessGroupEntity
     * @param state
     */
    private RemoteProcessGroupEntity enableOrDisableRemoteProcessGroupComponents(
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
        HttpHeaders headers = getAuthorizationHeader();
        HttpEntity<RemoteProcessGroupEntity> requestEntity = new HttpEntity<>(remoteProcessGroupEntityReq, headers);

        HttpEntity<RemoteProcessGroupEntity> response = restTemplate.exchange(uri, HttpMethod.PUT, requestEntity,
                RemoteProcessGroupEntity.class, params);

        RemoteProcessGroupEntity resp = response.getBody();

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
    private PortEntity deleteInputPortEntity(PortEntity portEntity, String state) {

        String peId = portEntity.getId();

        // https://"+nifiServerHostnameAndPort+"/nifi-api/process-groups/a57d7d2a-86bd-4b43-357a-34abb1bd85d6?version=0&clientId=deaebc77-015b-1000-31ea-162516e98255
        String version = String.valueOf(getClientIdAndVersion(portEntity).getVersion());
        String clientId = String.valueOf(getClientIdAndVersion(portEntity).getClientId());

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/input-ports/" + peId + "?version="
                + version + "&clientId=" + clientId;

        Map<String, String> params = new HashMap<String, String>();

        HttpHeaders requestHeaders = getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        PortEntity resp = null;
        HttpEntity<PortEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity, PortEntity.class,
                params);

        resp = response.getBody();

        logger.debug(resp.toString());
        return resp;

    }

    /**
     * Call the NIFI rest api to delete the Output port
     * https://localhost:8080/nifi-api/output-ports/ca8915b9-30be-1fca-8f44-56817f87fea1?version=1&clientId=ca8915b0-30be-1fca-4c85-739031a5f7cf
     *
     * @param portEntity
     * @param state
     * @return
     */
    private PortEntity deleteOutputPortEntity(PortEntity portEntity, String state) {

        String peId = portEntity.getId();

        // https://"+nifiServerHostnameAndPort+"/nifi-api/process-groups/a57d7d2a-86bd-4b43-357a-34abb1bd85d6?version=0&clientId=deaebc77-015b-1000-31ea-162516e98255
        String version = String.valueOf(getClientIdAndVersion(portEntity).getVersion());
        String clientId = String.valueOf(getClientIdAndVersion(portEntity).getClientId());

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/output-ports/" + peId + "?version="
                + version + "&clientId=" + clientId;

        Map<String, String> params = new HashMap<String, String>();

        HttpHeaders requestHeaders = getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        PortEntity resp = null;
        HttpEntity<PortEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity, PortEntity.class,
                params);

        resp = response.getBody();

        logger.debug(resp.toString());
        return resp;

    }

    /**
     * Call the NIFI rest api to delete the Processor
     * https://localhost:8080/nifi-api/processors/01213907-015b-1000-2760-95063f855d50?version=0&clientId=ca8915b0-30be-1fca-4c85-739031a5f7cf
     *
     * @param processor
     * @param state
     * @return
     */
    private ProcessorEntity deleteProcessorEntity(ProcessorEntity processor, String state) {

        String peId = processor.getId();

        // https://"+nifiServerHostnameAndPort+"/nifi-api/process-groups/a57d7d2a-86bd-4b43-357a-34abb1bd85d6?version=0&clientId=deaebc77-015b-1000-31ea-162516e98255
        String version = String.valueOf(getClientIdAndVersion(processor).getVersion());
        String clientId = String.valueOf(getClientIdAndVersion(processor).getClientId());

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/processors/" + peId + "?version="
                + version + "&clientId=" + clientId;

        Map<String, String> params = new HashMap<String, String>();

        HttpHeaders requestHeaders = getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        ProcessorEntity resp = null;
        HttpEntity<ProcessorEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity,
                ProcessorEntity.class, params);

        resp = response.getBody();

        logger.debug(resp.toString());
        return resp;

    }



    /**
     * This is the method to upload the template
     * https://"+nifiServerHostnameAndPort+"/nifi-api/process-groups/48e5e4b3-015b-1000-cd5b-ae5d2fdf9b54/templates/upload
     *
     * @param processGroupEntity
     */
    private TemplateEntity uploadTemplate(ProcessGroupEntity processGroupEntity) throws Exception {

        Resource resource = loadResourceUsingLoadFromParam();

        InputStream stream = resource.getInputStream();

        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<String, Object>();
        parts.add("template", new ByteArrayResource(IOUtils.toByteArray(stream)));
        parts.add("filename", resource.getFilename());

        HttpHeaders headers = getAuthorizationHeader();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<MultiValueMap<String, Object>>(parts,
                headers);

        String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/process-groups/" + processGroupEntity.getId()
                + "/templates/upload";

        // String strTemplateEntity= restTemplate.postForObject(uri,
        // requestEntity, String.class);
        ResponseEntity<TemplateEntity> response = restTemplate.exchange(uri, HttpMethod.POST, requestEntity,
                TemplateEntity.class);

        // JAXBContext jaxbContext =
        // JAXBContext.newInstance(TemplateEntity.class);
        // Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        // StringReader readerTemplateEntity = new
        // StringReader(strTemplateEntity);
        // TemplateEntity templateEntity = (TemplateEntity)
        // unmarshaller.unmarshal(readerTemplateEntity);
        //

        TemplateEntity templateEntity = response.getBody();
        logger.debug(templateEntity.toString());

        return templateEntity;

    }

    /**
     * Method is used to create the template instance based on Template ID
     *
     * @param processGroupEntity
     * @param templateId
     * @return
     */
    private FlowEntity createTemplateInstanceByTemplateId(ProcessGroupEntity processGroupEntity, String templateId) {

        String pgId = processGroupEntity.getId();

        InstantiateTemplateRequestEntity instantiateTemplateRequestEntity = new InstantiateTemplateRequestEntity();

        instantiateTemplateRequestEntity.setTemplateId(templateId);

        // Critical X axis and Yaxis - fields are mandatory
        instantiateTemplateRequestEntity.setOriginX(getXaxis());
        instantiateTemplateRequestEntity.setOriginY(getYaxis());

        HttpHeaders requestHeaders = getAuthorizationHeader();
        HttpEntity<InstantiateTemplateRequestEntity> requestEntity = new HttpEntity<InstantiateTemplateRequestEntity>(
                instantiateTemplateRequestEntity, requestHeaders);

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/process-groups/" + pgId
                + "/template-instance/";
        FlowEntity flowEntity = restTemplate.postForObject(uri, requestEntity, FlowEntity.class);

        return flowEntity;
    }

    /**
     * Check the Inputport service entity status
     *
     * @param controllerServiceEntity
     * @param state
     */
    private PortEntity checkInputPortStatus(PortEntity portEntity, String state) {
        int count = 0;

        PortEntity pe = null;

        while (true && count < WAIT_IN_SEC) {
            pe = getLatestInputPortEntity(portEntity);

            if (state.equalsIgnoreCase(pe.getComponent().getState()))
                break;

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {

            }

            count++;
        }

        return pe;

    }

    /**
     * Check the Output Port service entity status
     *
     * @param controllerServiceEntity
     * @param state
     */
    private PortEntity checkOutputPortStatus(PortEntity portEntity, String state) {
        int count = 0;

        PortEntity pe = null;

        while (true && count < WAIT_IN_SEC) {
            pe = getLatestOutputPortEntity(portEntity);

            if (state.equalsIgnoreCase(pe.getComponent().getState()))
                break;

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {

            }

            count++;
        }

        return pe;

    }

    /**
     * Check the Output Port service entity status
     *
     * @param processorEntity
     * @param state
     */
    private ProcessorEntity checkProcessorEntityStatus(ProcessorEntity processorEntity, String state) {
        int count = 0;

        ProcessorEntity pe = null;

        while (true && count < WAIT_IN_SEC) {
            pe = getLatestProcessorEntity(processorEntity);

            if (state.equalsIgnoreCase(pe.getComponent().getState()))
                break;

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {

            }

            count++;
        }

        return pe;

    }

    /**
     * Check the remote Process Group Component Status
     *
     * @param remoteProcessGroupEntity
     * @param state
     */
    private RemoteProcessGroupEntity checkRemoteProcessGroupComponentsStatus(
            RemoteProcessGroupEntity remoteProcessGroupEntity, String state) {
        int count = 0;

        RemoteProcessGroupEntity rpge = null;

        while (true && count < WAIT_IN_SEC) {
            rpge = getLatestRemoteProcessGroupEntity(remoteProcessGroupEntity.getId());

            if (state.equalsIgnoreCase(String.valueOf(rpge.getComponent().isTransmitting())))
                break;

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {

            }

            count++;
        }

        return rpge;

    }

    /**
     * Check for the reference component. check for the state else sleep for 10
     * sec
     *
     * @param controllerServiceEntity
     * @param state
     */
    private void checkReferenceComponentStatus(ControllerServiceEntity controllerServiceEntity, String state) {
        int count = 0;
        int innerCount = 0;
        ControllerServiceEntity cse = null;

        while (true && count < WAIT_IN_SEC) {
            cse = getLatestControllerServiceEntity(controllerServiceEntity);

            Set<ControllerServiceReferencingComponentEntity> referencingComponents = cse.getComponent()
                    .getReferencingComponents();

            for (ControllerServiceReferencingComponentEntity csrRefComp : referencingComponents) {

                if (!state.equalsIgnoreCase(csrRefComp.getComponent().getState())) {
                    break;
                }
                innerCount++;
            }

            if (referencingComponents.size() == innerCount) {
                break;
            }

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {

            }
            count++;
            innerCount = 0;
        }

    }

    /**
     * Check the Process Group Component Status
     *
     * @param processGroupFlowEntity
     * @param state
     */
    private void checkProcessGroupComponentStatus(ProcessGroupFlowEntity processGroupFlowEntity, String state,
                                                  String pgId) {
        checkInternalProcessGroupStatus(processGroupFlowEntity, state);

        ProcessGroupEntity pge = getLatestProcessGroupEntity(pgId);

        checkParentProcessGroupStatus(pge, state);
    }

    /**
     * This is the method to delete teh root proces group queu content
     *
     * @param rootPgId
     * @param state
     */
    private void deleteRootProcessGroupQueueContentIfAny(String rootPgId) {
        ProcessGroupFlowEntity pgfe = null;
        pgfe = getLatestProcessGroupFlowEntity(rootPgId);

        Set<ConnectionEntity> connections = pgfe.getProcessGroupFlow().getFlow().getConnections();

        int queuedCountInConnections = 0;
        DropRequestEntity dre = null;
        for (ConnectionEntity connection : connections) {
            queuedCountInConnections = Integer.parseInt(connection.getStatus().getAggregateSnapshot().getQueuedCount().replaceAll(",", ""));
            if (queuedCountInConnections > 0) {
                dre = placeRequestForDeletion(connection);
                dre = deleteTheQueueContent(dre);
            }
        }


    }


    private void checkParentProcessGroupStatus(ProcessGroupEntity pge, String state) {
        int count = 0;
        int innerCount = 0;

        while (true && count < WAIT_IN_SEC) {

            Set<ProcessGroupEntity> processGroups = new LinkedHashSet<>();
            processGroups.add(pge);

            int queuedCount = 0;
            for (ProcessGroupEntity processGroupEntity : processGroups) {
                if (state.equalsIgnoreCase(EntityState.STOPPED.getState())) {
                    queuedCount = Integer
                            .parseInt(processGroupEntity.getStatus().getAggregateSnapshot().getQueuedCount().replaceAll(",", ""));
                    // Check for the Runing count
                    if (processGroupEntity.getRunningCount() > 0) {
                        break;
                    }
                    // Check for the queue content
                    if (queuedCount > 0) {
                        deleteTheQueueContent(processGroupEntity);
                        break;
                    }

                }

                if (state.equalsIgnoreCase(EntityState.RUNNING.getState())
                        && processGroupEntity.getStoppedCount() > 0) {
                    break;
                }

                innerCount++;
            }

            if (processGroups.size() == innerCount) {
                break;
            }

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {

            }
            pge = getLatestProcessGroupEntity(pge.getId());
            count++;
            innerCount = 0;
        }

    }

    private void checkInternalProcessGroupStatus(ProcessGroupFlowEntity processGroupFlowEntity, String state) {
        int count = 0;
        int innerCount = 0;
        ProcessGroupFlowEntity pgfe = null;
        //ProcessGroupFlowEntity currentPgfe = null;


        while (true && count < WAIT_IN_SEC) {
            pgfe = getLatestProcessGroupFlowEntity(processGroupFlowEntity.getProcessGroupFlow().getId());

            Set<ProcessGroupEntity> processGroups = pgfe.getProcessGroupFlow().getFlow().getProcessGroups();

            int queuedCount = 0;
            for (ProcessGroupEntity processGroupEntity : processGroups) {

				/*if(! processGroupEntity.getComponent().getContents().getProcessGroups().isEmpty()){
					currentPgfe = getLatestProcessGroupFlowEntity(processGroupEntity.getId());
					checkInternalProcessGroupStatus(currentPgfe, state);
				}*/
                /*
                 * Stop only the necessary process groups for the given process
                 * group ID
                 */
                if (processGroupEntity.getComponent().getParentGroupId()
                        .equalsIgnoreCase(processGroupFlowEntity.getProcessGroupFlow().getId())) {

                    if (state.equalsIgnoreCase(EntityState.STOPPED.getState())) {
                        queuedCount = Integer
                                .parseInt(processGroupEntity.getStatus().getAggregateSnapshot().getQueuedCount().replaceAll(",", ""));
                        // Check for the Runing count
                        if (processGroupEntity.getRunningCount() > 0) {
                            break;
                        }
                        // Check for the queue content
                        if (queuedCount > 0) {
                            deleteTheQueueContent(processGroupEntity);
                            break;
                        }

                    }

                    if (state.equalsIgnoreCase(EntityState.RUNNING.getState())
                            && processGroupEntity.getStoppedCount() > 0) {
                        break;
                    }
                }
                innerCount++;
            }

            if (processGroups.size() == innerCount) {
                break;
            }

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {

            }
            count++;
            innerCount = 0;
        }
    }

    /**
     * Check the controller service entity status
     *
     * @param controllerServiceEntity
     * @param state
     */
    private void checkControllerServiceStatus(ControllerServiceEntity controllerServiceEntity, String state) {
        int count = 0;

        ControllerServiceEntity cse = null;

        while (true && count < WAIT_IN_SEC) {
            cse = getLatestControllerServiceEntity(controllerServiceEntity);

            if (state.equalsIgnoreCase(cse.getComponent().getState())) {
                break;
            }

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {

            }

            count++;
        }

    }

    /**
     * Delete the queue content for the process group entity
     *
     * @param processGroupEntity
     */
    private void deleteTheQueueContent(ProcessGroupEntity pge) {
        if (deleteQueueContent == false)
            throw new RuntimeException("Queues Are Not Empty.. Please flush the queus manually before deletion...");

        ProcessGroupFlowEntity pgfe = getLatestProcessGroupFlowEntity(pge.getId());
        Set<ProcessGroupEntity> processGroups = pgfe.getProcessGroupFlow().getFlow().getProcessGroups();

        int queuedCount = 0;

        for (ProcessGroupEntity processGroupEntity : processGroups) {
            queuedCount = Integer.parseInt(processGroupEntity.getStatus().getAggregateSnapshot().getQueuedCount().replaceAll(",", ""));

            if (queuedCount > 0) {
                deleteTheQueueContent(processGroupEntity);
            }
        }

        Set<ConnectionEntity> connections = pgfe.getProcessGroupFlow().getFlow().getConnections();

        int queuedCountInConnections = 0;
        DropRequestEntity dre = null;
        for (ConnectionEntity connection : connections) {
            queuedCountInConnections = Integer.parseInt(connection.getStatus().getAggregateSnapshot().getQueuedCount().replaceAll(",", ""));
            if (queuedCountInConnections > 0) {
                dre = placeRequestForDeletion(connection);
                dre = deleteTheQueueContent(dre);
            }
        }

    }

    /**
     * Check the template exists
     *
     * @throws IOException
     * @throws JAXBException
     */
    private String checkTemplateExist() throws IOException, JAXBException {
        Resource resource = loadResourceUsingLoadFromParam();
        InputStream inputStream = resource.getInputStream();

        JAXBContext jaxbContext = JAXBContext.newInstance(TemplateDTO.class);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        TemplateDTO inputTemplateDTO = (TemplateDTO) unmarshaller.unmarshal(inputStream);

        TemplatesEntity templatesEntity = getAllTemplates();

        Set<TemplateEntity> templates = templatesEntity.getTemplates();

        TemplateDTO templateDTO = null;
        for (TemplateEntity template : templates) {
            templateDTO = template.getTemplate();

            if (templateDTO.getName().equalsIgnoreCase(inputTemplateDTO.getName()))
                return templateDTO.getId();

        }

        return null;

    }

    /**
     * This is the method which is used to read the template from Load from
     * Param of the property FIle
     *
     * @return
     * @throws IOException
     * @throws JAXBException
     */
    private TemplateDTO readTemplateUsingLoadFromParam() {
        TemplateDTO inputTemplateDTO = null;
        try {
            Resource resource = loadResourceUsingLoadFromParam();
            InputStream inputStream = resource.getInputStream();

            JAXBContext jaxbContext = JAXBContext.newInstance(TemplateDTO.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            inputTemplateDTO = (TemplateDTO) unmarshaller.unmarshal(inputStream);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Unable to parse the Input Template. Please upload the Valid template .." + templateFileLocation);
        }
        return inputTemplateDTO;
    }





    /**
     * This is the method to get the latest Input Port Entity
     *
     * @param portEntity
     * @return
     */
    private PortEntity getLatestInputPortEntity(PortEntity portEntity) {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/input-ports/" + portEntity.getId() + "/";
        HttpEntity<PortEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity, PortEntity.class,
                params);
        return response.getBody();
    }

    /**
     * This is the method to get the latest Output port entity
     *
     * @param portEntity
     * @return
     */
    private PortEntity getLatestOutputPortEntity(PortEntity portEntity) {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/output-ports/" + portEntity.getId() + "/";
        HttpEntity<PortEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity, PortEntity.class,
                params);
        return response.getBody();
    }

    /**
     * This is the method to get the latest Processor Entity
     *
     * @param processorEntity https://localhost:8080/nifi-api/processors/
     * @return
     */
    private ProcessorEntity getLatestProcessorEntity(ProcessorEntity processorEntity) {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/processors/" + processorEntity.getId()
                + "/";
        HttpEntity<ProcessorEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                ProcessorEntity.class, params);
        return response.getBody();
    }




    /**
     * This is the method which is used to get the remote process groups for the
     * Process GROUP ID .. /process-groups/{id}/remote-process-groups
     *
     * @param pgId
     * @return
     */
    private RemoteProcessGroupsEntity getLatestRemoteProcessGroupsEntity(String pgId) {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/process-groups/" + pgId
                + "/remote-process-groups/";
        HttpEntity<RemoteProcessGroupsEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                RemoteProcessGroupsEntity.class, params);
        return response.getBody();
    }

    /**
     * This is the method which is used to get the get the remote process groups
     * for the remote process grp ID ..
     *
     * @param id
     * @return /remote-process-groups/{id} Gets a remote process group
     */
    private RemoteProcessGroupEntity getLatestRemoteProcessGroupEntity(String rpgeId) {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/remote-process-groups/" + rpgeId + "/";
        HttpEntity<RemoteProcessGroupEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                RemoteProcessGroupEntity.class, params);
        return response.getBody();
    }



    /**
     * This is the method to get all the templates
     *
     * @param processGroupEntity
     */
    private TemplatesEntity getAllTemplates() {
        // https://"+nifiServerHostnameAndPort+"/nifi-api/flow/templates
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flow/templates/";
        HttpEntity<TemplatesEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                TemplatesEntity.class, params);
        return response.getBody();
    }

    /**
     * This is the method to delete the template that already exists
     *
     * @param processGroupEntity https://"+nifiServerHostnameAndPort+"/nifi-api/templates/
     */
    private void deleteTemplate(String templateId) {

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/templates/" + templateId + "/";

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        HttpEntity<TemplateEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity,
                TemplateEntity.class, params);

        TemplateEntity resp = response.getBody();
        logger.debug(resp.toString());

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

        HttpHeaders requestHeaders = getAuthorizationHeader();
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

        HttpHeaders requestHeaders = getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        HttpEntity<DropRequestEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity,
                DropRequestEntity.class, params);

        DropRequestEntity resp = response.getBody();

        logger.debug(resp.toString());

        return resp;
    }








    private Set<String> getAllInputPortNameFromTemplate(Set<PortDTO> inputPortFromTemplate) {
        Set<String> inputPortNameFromTemplate = new LinkedHashSet<>();

        for (PortDTO portDTO : inputPortFromTemplate) {
            inputPortNameFromTemplate.add(portDTO.getName());
        }

        return inputPortNameFromTemplate;
    }

    private Set<String> getAllOutputPortNameFromTemplate(Set<PortDTO> outputPortFromTemplate) {
        Set<String> outputPortNameFromTemplate = new LinkedHashSet<>();

        for (PortDTO portDTO : outputPortFromTemplate) {
            outputPortNameFromTemplate.add(portDTO.getName());
        }

        return outputPortNameFromTemplate;
    }

    private Set<String> getAllProcessorsNameFromTemplate(Set<ProcessorDTO> processorsFromTemplate) {
        Set<String> processorNameFromTemplate = new LinkedHashSet<>();

        for (ProcessorDTO processorDTO : processorsFromTemplate) {
            processorNameFromTemplate.add(processorDTO.getName());
        }

        return processorNameFromTemplate;
    }

    /**
     * @param remoteProcessGroupsFromTemplate
     * @return
     */
    private Set<String> getAllRemoteProcessorGroupNameFromTemplate(
            Set<RemoteProcessGroupDTO> remoteProcessGroupsFromTemplate) {
        Set<String> remoteProcessorGroupNameFromTemplate = new LinkedHashSet<>();

        for (RemoteProcessGroupDTO remoteProcessGroupDTO : remoteProcessGroupsFromTemplate) {
            remoteProcessorGroupNameFromTemplate.add(remoteProcessGroupDTO.getName());
        }

        return remoteProcessorGroupNameFromTemplate;
    }





}
