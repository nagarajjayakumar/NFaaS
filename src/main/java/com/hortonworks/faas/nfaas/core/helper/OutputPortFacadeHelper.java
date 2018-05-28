package com.hortonworks.faas.nfaas.core.helper;

import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.LinkedHashSet;
import java.util.Set;

public class OutputPortFacadeHelper {

    private static final Logger logger = LoggerFactory.getLogger(OutputPortFacadeHelper.class);

    @Autowired
    TemplateFacadeHelper templateFacadeHelper;

    /**
     * This is the method to get all the Output port entity from the pgfe
     *
     * @param pgfe
     * @param outputPortFromTemplate
     * @return
     */
    public Set<PortEntity> getOutputPortsEntityForUndeploy(ProcessGroupFlowEntity pgfe,
                                                            Set<PortDTO> outputPortFromTemplate) {

        Set<PortEntity> resultOutputPorts = new LinkedHashSet<>();
        Set<PortEntity> allOutputPorts = pgfe.getProcessGroupFlow().getFlow().getOutputPorts();

        Set<String> outputPortsNameFromTemplate = templateFacadeHelper.getAllOutputPortNameFromTemplate(outputPortFromTemplate);

        for (PortEntity pe : allOutputPorts) {
            if (outputPortsNameFromTemplate.contains(pe.getComponent().getName())) {
                resultOutputPorts.add(pe);
            }

        }
        return resultOutputPorts;
    }

}
