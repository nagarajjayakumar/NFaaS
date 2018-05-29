package com.hortonworks.faas.nfaas.core.helper;

import com.hortonworks.faas.nfaas.config.EntityState;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Configuration
public class OutputPortFacadeHelper extends BaseFacadeHelper {

    private static final Logger logger = LoggerFactory.getLogger(OutputPortFacadeHelper.class);

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

    /**
     * This is the method which is used to start the Port Entity ,,,,
     *
     * @param portEntity
     * @return
     */
    public PortEntity startOutputPortEntity(PortEntity portEntity) {
        PortEntity pe = outputPort.getLatestOutputPortEntity(portEntity);
        pe = outputPort.startOrStopOutputPortEntity(pe, EntityState.RUNNING.getState());
        pe = this.checkOutputPortStatus(pe, EntityState.RUNNING.getState());
        return pe;
    }

    /**
     * This is the method which is used to stop the Port Entity ,,,,
     *
     * @param portEntity
     * @return
     */
    public PortEntity stopOutputPortEntity(PortEntity portEntity) {
        PortEntity pe = outputPort.getLatestOutputPortEntity(portEntity);
        pe = outputPort.startOrStopOutputPortEntity(pe, EntityState.STOPPED.getState());
        pe = this.checkOutputPortStatus(pe, EntityState.STOPPED.getState());
        return pe;
    }

    /**
     * This is the method which is used to stop the Port Entity ,,,,
     *
     * @param portEntity
     * @return
     */
    public void deleteOutputPortEntity(PortEntity portEntity) {
        PortEntity pe = outputPort.getLatestOutputPortEntity(portEntity);
        pe = outputPort.deleteOutputPortEntity(pe, EntityState.DELETE.getState());
        logger.info(pe.toString());
    }


    /**
     * Check the Output Port service entity status
     *
     * @param portEntity
     * @param state
     */
    public PortEntity checkOutputPortStatus(PortEntity portEntity, String state) {
        int count = 0;

        PortEntity pe = null;

        while (true && count < WAIT_IN_SEC) {
            pe = outputPort.getLatestOutputPortEntity(portEntity);

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

}
