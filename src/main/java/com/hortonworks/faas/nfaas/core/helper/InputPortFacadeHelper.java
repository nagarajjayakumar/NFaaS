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
public class InputPortFacadeHelper extends BaseFacadeHelper {

    private static final Logger logger = LoggerFactory.getLogger(InputPortFacadeHelper.class);

    /**
     * This is the method to get all the input port entity from the pgfe
     *
     * @param pgfe
     * @param inputPortFromTemplate
     * @return
     */
    public Set<PortEntity> getInputPortsEntityForUndeploy(ProcessGroupFlowEntity pgfe,
                                                          Set<PortDTO> inputPortFromTemplate) {

        Set<PortEntity> resultInputPorts = new LinkedHashSet<>();
        Set<PortEntity> allInputPorts = pgfe.getProcessGroupFlow().getFlow().getInputPorts();

        Set<String> inputPortsNameFromTemplate = templateFacadeHelper.getAllInputPortNameFromTemplate(inputPortFromTemplate);

        for (PortEntity pe : allInputPorts) {
            if (inputPortsNameFromTemplate.contains(pe.getComponent().getName())) {
                resultInputPorts.add(pe);
            }

        }
        return resultInputPorts;
    }

    /**
     * This is the method which is used to start the Port Entity ,,,,
     *
     * @param portEntity
     * @return
     */
    private PortEntity startInputPortEntity(PortEntity portEntity) {
        PortEntity pe = inputPort.getLatestInputPortEntity(portEntity);
        pe = inputPort.startOrStopInputPortEntity(pe, EntityState.RUNNING.getState());
        pe = this.checkInputPortStatus(pe, EntityState.RUNNING.getState());
        return pe;
    }

    /**
     * This is the method which is used to stop the Port Entity ,,,,
     *
     * @param portEntity
     * @return
     */
    private PortEntity stopInputPortEntity(PortEntity portEntity) {
        PortEntity pe = inputPort.getLatestInputPortEntity(portEntity);
        pe = inputPort.startOrStopInputPortEntity(pe, EntityState.STOPPED.getState());
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
        PortEntity pe = inputPort.getLatestInputPortEntity(portEntity);
        pe = inputPort.deleteInputPortEntity(pe, EntityState.DELETE.getState());
        logger.info(pe.toString());
    }


    /**
     * Check the Inputport service entity status
     *
     * @param portEntity
     * @param state
     */
    private PortEntity checkInputPortStatus(PortEntity portEntity, String state) {
        int count = 0;

        PortEntity pe = null;

        while (true && count < WAIT_IN_SEC) {
            pe = inputPort.getLatestInputPortEntity(portEntity);

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
