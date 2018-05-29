package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.util.Random;

@Configuration
public class CommonService {

    private static final Logger logger = LoggerFactory.getLogger(CommonService.class);

    Environment env;

    @Autowired
    private ResourceLoader resourceLoader;

    private String templateFileLocation = "classpath:Hello_NiFi_Web_Service.xml";
    private String templateFileURI = "https://cwiki.apache.org/confluence/download/attachments/57904847/Hello_NiFi_Web_Service.xml?version=1&modificationDate=1449369797000&api=v2";
    private String templateFileLoadFrom = "FILE";


    @Autowired
    CommonService(Environment env) {

        logger.info("Intialized ProcessGroupFlow !!! ");
        this.env = env;
        this.templateFileLocation = env.getProperty("bootrest.templateFileLocation");
        this.templateFileURI = env.getProperty("bootrest.templateFileURI");
        this.templateFileLoadFrom = env.getProperty("bootrest.templateFileLoadFrom");
    }

    /**
     * Update the last Modified for the entity.
     *
     * @param entity
     * @return
     */
    public RevisionDTO updateLastModified(ComponentEntity entity) {
        RevisionDTO revision = getClientIdAndVersion(entity);
        revision.setLastModifier(getUser());
        return revision;
    }

    /**
     * This is the method to get the client ID and version for the Component
     * Entity
     *
     * @param entity
     * @return
     */
    public RevisionDTO getClientIdAndVersion(ComponentEntity entity) {
        RevisionDTO revision = new RevisionDTO();

        if (null != entity) {
            return entity.getRevision();
        }

        return revision;
    }

    public String getUser() {
        // TODO Auto-generated method stub
        return "hdfm";
    }

    public Double getXaxis() {
        double rangeMin = 1500;
        double rangeMax = 2000;
        Random r = new Random();
        double randomValue = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
        return randomValue;
    }

    public Double getYaxis() {
        double rangeMin = 1000;
        double rangeMax = 1500;
        Random r = new Random();
        double randomValue = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
        return randomValue;
    }

    public Resource loadResourceUsingLoadFromParam() {
        Resource resource = resourceLoader.getResource(templateFileLocation);

        if ("URI".equalsIgnoreCase(templateFileLoadFrom)) {
            resource = resourceLoader.getResource(templateFileURI);
        }
        return resource;
    }

    public String getTemplateFileLocation(){
        return this.templateFileLocation;
    }

}
