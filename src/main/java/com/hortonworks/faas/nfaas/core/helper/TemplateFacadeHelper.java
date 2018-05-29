package com.hortonworks.faas.nfaas.core.helper;

import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;

public class TemplateFacadeHelper {

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
     * Method is used to create the template Instance
     *
     * @param processGroupEntity
     * @return
     */
    public FlowEntity createTemplateInstanceByTemplateId(ProcessGroupEntity processGroupEntity) {

        String templateId = getTemplateId(processGroupEntity);

        if (null == templateId || templateId.isEmpty()) {
            throw new RuntimeException("Unable to upload the template ");
        }
        FlowEntity fe = createTemplateInstanceByTemplateId(processGroupEntity, templateId);
        return fe;
    }
}
