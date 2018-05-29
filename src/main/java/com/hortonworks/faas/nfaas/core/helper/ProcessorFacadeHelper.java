package com.hortonworks.faas.nfaas.core.helper;

import com.hortonworks.faas.nfaas.config.EntityState;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Configuration
public class ProcessorFacadeHelper extends BaseFacadeHelper{

    private static final Logger logger = LoggerFactory.getLogger(ProcessorFacadeHelper.class);


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

    /**
     * This is the method to start all the Processors
     *
     * @param processorGroup
     * @return
     */
    private ProcessGroupFlowEntity startAllProcessors(ProcessGroupEntity processorGroup) {

        logger.info("Process Group Starting Starts --> " + processorGroup.getComponent().getName());
        ProcessGroupFlowEntity processGroupFlowEntity = processGroupFlow.getLatestProcessGroupFlowEntity(processorGroup.getId());

        processorGroupFlowFacadeHelper.startProcessGroupComponents(processGroupFlowEntity, EntityState.RUNNING.getState());
        processorGroupFlowFacadeHelper.checkProcessGroupComponentStatus(processGroupFlowEntity, EntityState.RUNNING.getState(), processorGroup.getId());

        ProcessGroupFlowEntity pge = processGroupFlow.getLatestProcessGroupFlowEntity(
                processGroupFlowEntity.getProcessGroupFlow().getId());
        logger.info("Process Group Starting Ends  --> " + processorGroup.getComponent().getName());
        return pge;

    }



    /**
     * This is the method which is used to Start the Processor Entity ,,,,
     *
     * @param processorEntity
     * @return
     */
    private ProcessorEntity startProcessorEntity(ProcessorEntity processorEntity) {
        ProcessorEntity pe = processor.getLatestProcessorEntity(processorEntity);
        if (EntityState.INVALID.getState().equalsIgnoreCase(pe.getStatus().getAggregateSnapshot().getRunStatus())) {
            logger.error("Procesor is in invalid state .. unable to start  " + pe.getComponent().getName());
            return pe;
        }
        pe = processor.startOrStopProcessorEntity(pe, EntityState.RUNNING.getState());
        pe = this.checkProcessorEntityStatus(pe, EntityState.RUNNING.getState());
        return pe;
    }

    /**
     * This is the method which is used to stop the Processor Entity ,,,,
     *
     * @param processorEntity
     * @return
     */
    private ProcessorEntity stopProcessorEntity(ProcessorEntity processorEntity) {
        ProcessorEntity pe = processor.getLatestProcessorEntity(processorEntity);
        if (EntityState.INVALID.getState().equalsIgnoreCase(pe.getStatus().getAggregateSnapshot().getRunStatus())) {
            logger.error("Procesor is in invalid state unable to stop " + pe.getComponent().getName());
            return pe;
        }
        pe = processor.startOrStopProcessorEntity(pe, EntityState.STOPPED.getState());
        pe = this.checkProcessorEntityStatus(pe, EntityState.STOPPED.getState());
        return pe;
    }

    /**
     * This is the method which is used to stop the Processor Entity ,,,,
     *
     * @param processorEntity
     * @return
     */
    private void deleteProcessorEntity(ProcessorEntity processorEntity) {
        ProcessorEntity pe = processor.getLatestProcessorEntity(processorEntity);
        pe = processor.deleteProcessorEntity(pe, EntityState.DELETE.getState());
        logger.info(pe.toString());
    }

    /**
     * This is the method to Stop all processors for the process group
     *
     * @param pgId
     * @return
     */
    private ProcessGroupFlowEntity stopAllProcessors(String pgId) {
        ProcessGroupFlowEntity pgfe = processGroupFlow.getLatestProcessGroupFlowEntity(pgId);
        processGroupFacadeHelper.stopProcessGroup(pgfe, pgId);
        return pgfe;
    }

    /**
     * This is the method to Stop all processors for the process group
     *
     * @param pgId
     * @return
     */
    private ProcessGroupFlowEntity deleteAllProcessors(String pgId) {
        ProcessGroupFlowEntity pgfe = processGroupFlow.getLatestProcessGroupFlowEntity(pgId);
        processGroupFacadeHelper.deleteProcessGroup(pgfe, pgId);
        return pgfe;
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
            pe = processor.getLatestProcessorEntity(processorEntity);

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

