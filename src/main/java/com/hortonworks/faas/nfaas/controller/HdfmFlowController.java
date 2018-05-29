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







}
