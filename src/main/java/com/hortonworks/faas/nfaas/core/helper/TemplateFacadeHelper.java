package com.hortonworks.faas.nfaas.core.helper;

import org.apache.nifi.web.api.dto.*;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.TemplateEntity;
import org.apache.nifi.web.api.entity.TemplatesEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Set;

@Configuration
public class TemplateFacadeHelper extends  BaseFacadeHelper{

    private static final Logger logger = LoggerFactory.getLogger(TemplateFacadeHelper.class);

    public Set<String> getAllInputPortNameFromTemplate(Set<PortDTO> inputPortFromTemplate) {
        Set<String> inputPortNameFromTemplate = new LinkedHashSet<>();

        for (PortDTO portDTO : inputPortFromTemplate) {
            inputPortNameFromTemplate.add(portDTO.getName());
        }

        return inputPortNameFromTemplate;
    }

    public Set<String> getAllOutputPortNameFromTemplate(Set<PortDTO> outputPortFromTemplate) {
        Set<String> outputPortNameFromTemplate = new LinkedHashSet<>();

        for (PortDTO portDTO : outputPortFromTemplate) {
            outputPortNameFromTemplate.add(portDTO.getName());
        }

        return outputPortNameFromTemplate;
    }

    public Set<String> getAllProcessorsNameFromTemplate(Set<ProcessorDTO> processorsFromTemplate) {
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
    public Set<String> getAllRemoteProcessorGroupNameFromTemplate(
            Set<RemoteProcessGroupDTO> remoteProcessGroupsFromTemplate) {
        Set<String> remoteProcessorGroupNameFromTemplate = new LinkedHashSet<>();

        for (RemoteProcessGroupDTO remoteProcessGroupDTO : remoteProcessGroupsFromTemplate) {
            remoteProcessorGroupNameFromTemplate.add(remoteProcessGroupDTO.getName());
        }

        return remoteProcessorGroupNameFromTemplate;
    }

    /**
     * @param connectionsFromTemplate
     * @return
     */
    public Set<String> getAllConnectionsFromTemplate(
            Set<ConnectionDTO> connectionsFromTemplate) {
        Set<String> connectionsNameFromTemplate = new LinkedHashSet<>();

        for (ConnectionDTO connectionDTO : connectionsFromTemplate) {
            connectionsNameFromTemplate.add(connectionDTO.getName());
        }

        return connectionsNameFromTemplate;
    }


    /**
     * Method is used to create the template Instance
     *
     * @param processGroupEntity
     * @return
     */
    public FlowEntity createTemplateInstanceByTemplateId(ProcessGroupEntity processGroupEntity) {

        String templateId = this.getTemplateId(processGroupEntity);

        if (null == templateId || templateId.isEmpty()) {
            throw new RuntimeException("Unable to upload the template ");
        }
        FlowEntity fe = template.createTemplateInstanceByTemplateId(processGroupEntity, templateId);
        return fe;
    }


    /**
     * Check the template exists
     *
     * @throws IOException
     * @throws JAXBException
     */
    public String checkTemplateExist() throws IOException, JAXBException {
        Resource resource = commonService.loadResourceUsingLoadFromParam();
        InputStream inputStream = resource.getInputStream();

        JAXBContext jaxbContext = JAXBContext.newInstance(TemplateDTO.class);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        TemplateDTO inputTemplateDTO = (TemplateDTO) unmarshaller.unmarshal(inputStream);

        TemplatesEntity templatesEntity = template.getAllTemplates();

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
    public TemplateDTO readTemplateUsingLoadFromParam() {
        TemplateDTO inputTemplateDTO = null;
        try {
            Resource resource = commonService.loadResourceUsingLoadFromParam();
            InputStream inputStream = resource.getInputStream();

            JAXBContext jaxbContext = JAXBContext.newInstance(TemplateDTO.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            inputTemplateDTO = (TemplateDTO) unmarshaller.unmarshal(inputStream);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Unable to parse the Input Template. Please upload the Valid template .." + commonService.getTemplateFileLocation());
        }
        return inputTemplateDTO;
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
                template.deleteTemplate(templateId);

            TemplateEntity templateEntity = template.uploadTemplate(processGroupEntity);

            return templateEntity.getTemplate().getId();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return "";
    }


}
