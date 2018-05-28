package com.hortonworks.faas.nfaas.core.helper;

import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.LinkedHashSet;
import java.util.Set;

public class ProcessorFacadeHelper {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorFacadeHelper.class);

    @Autowired
    TemplateFacadeHelper templateFacadeHelper;

    /**
     * This is the method to get all the Processor entity from the pgfe
     *
     * @param pgfe
     * @param processorsFromTemplate
     * @return
     */
    public Set<ProcessorEntity> getProcessorEntityForUndeploy(ProcessGroupFlowEntity pgfe,
                                                               Set<ProcessorDTO> processorsFromTemplate) {

        Set<ProcessorEntity> resultProcessors = new LinkedHashSet<>();
        Set<ProcessorEntity> allProcessGroups = pgfe.getProcessGroupFlow().getFlow().getProcessors();

        Set<String> processorNameFromTemplate = templateFacadeHelper.getAllProcessorsNameFromTemplate(processorsFromTemplate);

        for (ProcessorEntity pe : allProcessGroups) {
            if (processorNameFromTemplate.contains(pe.getComponent().getName())) {
                resultProcessors.add(pe);
            }

        }
        return resultProcessors;
    }

}

