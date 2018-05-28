package com.hortonworks.faas.nfaas.core;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.web.api.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class Template {

    private static final Logger logger = LoggerFactory.getLogger(Template.class);

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
    Template(Environment env) {
        logger.info("Intialized Template !!! ");
        this.env = env;
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
    }

    /**
     * This is the method to upload the template
     * https://"+nifiServerHostnameAndPort+"/nifi-api/process-groups/48e5e4b3-015b-1000-cd5b-ae5d2fdf9b54/templates/upload
     *
     * @param processGroupEntity
     */
    public TemplateEntity uploadTemplate(ProcessGroupEntity processGroupEntity) throws Exception {

        Resource resource = commonService.loadResourceUsingLoadFromParam();

        InputStream stream = resource.getInputStream();

        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<String, Object>();
        parts.add("template", new ByteArrayResource(IOUtils.toByteArray(stream)));
        parts.add("filename", resource.getFilename());

        HttpHeaders headers = security.getAuthorizationHeader();
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
    public FlowEntity createTemplateInstanceByTemplateId(ProcessGroupEntity processGroupEntity, String templateId) {

        String pgId = processGroupEntity.getId();

        InstantiateTemplateRequestEntity instantiateTemplateRequestEntity = new InstantiateTemplateRequestEntity();

        instantiateTemplateRequestEntity.setTemplateId(templateId);

        // Critical X axis and Yaxis - fields are mandatory
        instantiateTemplateRequestEntity.setOriginX(commonService.getXaxis());
        instantiateTemplateRequestEntity.setOriginY(commonService.getYaxis());

        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<InstantiateTemplateRequestEntity> requestEntity = new HttpEntity<InstantiateTemplateRequestEntity>(
                instantiateTemplateRequestEntity, requestHeaders);

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/process-groups/" + pgId
                + "/template-instance/";
        FlowEntity flowEntity = restTemplate.postForObject(uri, requestEntity, FlowEntity.class);

        return flowEntity;
    }

    /**
     * This is the method to get all the templates
     * https://"+nifiServerHostnameAndPort+"/nifi-api/flow/templates
     *
     * @param
     */
    public TemplatesEntity getAllTemplates() {
        // https://"+nifiServerHostnameAndPort+"/nifi-api/flow/templates
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/flow/templates/";
        HttpEntity<TemplatesEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity,
                TemplatesEntity.class, params);
        return response.getBody();
    }

    /**
     * This is the method to delete the template that already exists
     *
     * @param templateId
     *
     * https://"+nifiServerHostnameAndPort+"/nifi-api/templates/
     */
    public void deleteTemplate(String templateId) {

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/templates/" + templateId + "/";

        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        HttpEntity<TemplateEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity,
                TemplateEntity.class, params);

        TemplateEntity resp = response.getBody();
        logger.debug(resp.toString());

    }

}
