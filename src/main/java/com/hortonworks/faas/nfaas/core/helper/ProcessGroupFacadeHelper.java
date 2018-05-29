package com.hortonworks.faas.nfaas.core.helper;

import com.hortonworks.faas.nfaas.config.EntityState;
import com.hortonworks.faas.nfaas.core.FlowFileQueue;
import com.hortonworks.faas.nfaas.core.ProcessGroup;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.TemplateEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ProcessGroupFacadeHelper {

    private static final Logger logger = LoggerFactory.getLogger(ProcessGroupFacadeHelper.class);

    private int WAIT_IN_SEC = 10;

    @Autowired
    ProcessorGroupFlowFacadeHelper processorGroupFlowFacadeHelper;

    @Autowired
    FlowFileQueue flowFileQueues;

    @Autowired
    ProcessGroup processGroup;

    /**
     * This is the method to stop and un-deploy the process group.
     *
     * @param processGroupFlowEntity
     */
    private void stopAndUnDeployProcessGroup(ProcessGroupFlowEntity processGroupFlowEntity, String pgId) {
        logger.info("stopAndUnDeployProcessGroup Starts for --> " + pgId);
        ProcessGroupFlowEntity pgfe = processorGroupFlowFacadeHelper.stopProcessGroupComponents(processGroupFlowEntity, null, pgId);
        logger.info(pgfe.toString());
        disableRemoteProcessGroup(pgId);
        ProcessGroupEntity pge = processGroup.getLatestProcessGroupEntity(pgId);
        pge = this.deleteProcessGroup(pge);
        logger.info("stopAndUnDeployProcessGroup Ends for --> " + pgId);
    }

    /**
     * This is the method to stop the process group.
     *
     * @param processGroupFlowEntity
     */
    private void stopProcessGroup(ProcessGroupFlowEntity processGroupFlowEntity, String pgId) {
        logger.info("stopProcessGroup Starts for --> " + pgId);
        ProcessGroupFlowEntity pgfe = processorGroupFlowFacadeHelper.stopProcessGroupComponents(processGroupFlowEntity, null, pgId);
        logger.info(pgfe.toString());
        ProcessGroupEntity pge = processGroup.getLatestProcessGroupEntity(pgId);
        logger.info("stopProcessGroup Ends for --> " + pge.getComponent().getName());
    }


    private void checkParentProcessGroupStatus(ProcessGroupEntity pge, String state) {
        int count = 0;
        int innerCount = 0;

        while (true && count < WAIT_IN_SEC) {

            Set<ProcessGroupEntity> processGroups = new LinkedHashSet<>();
            processGroups.add(pge);

            int queuedCount = 0;
            for (ProcessGroupEntity processGroupEntity : processGroups) {
                if (state.equalsIgnoreCase(EntityState.STOPPED.getState())) {
                    queuedCount = Integer
                            .parseInt(processGroupEntity.getStatus().getAggregateSnapshot().getQueuedCount().replaceAll(",", ""));
                    // Check for the Runing count
                    if (processGroupEntity.getRunningCount() > 0) {
                        break;
                    }
                    // Check for the queue content
                    if (queuedCount > 0) {
                        flowFileQueues.deleteTheQueueContent(processGroupEntity);
                        break;
                    }

                }

                if (state.equalsIgnoreCase(EntityState.RUNNING.getState())
                        && processGroupEntity.getStoppedCount() > 0) {
                    break;
                }

                innerCount++;
            }

            if (processGroups.size() == innerCount) {
                break;
            }

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {

            }
            pge = processGroup.getLatestProcessGroupEntity(pge.getId());
            count++;
            innerCount = 0;
        }

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
                deleteTemplate(templateId);

            TemplateEntity templateEntity = uploadTemplate(processGroupEntity);

            return templateEntity.getTemplate().getId();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return "";
    }
    /**
     * Check the Process Group Component Status
     *
     * @param processGroupFlowEntity
     * @param state
     */
    public void checkProcessGroupComponentStatus(ProcessGroupFlowEntity processGroupFlowEntity, String state,
                                                  String pgId) {
        processorGroupFlowFacadeHelper.checkInternalProcessGroupStatus(processGroupFlowEntity, state);

        ProcessGroupEntity pge = processGroup.getLatestProcessGroupEntity(pgId);

        checkParentProcessGroupStatus(pge, state);
    }

    /**
     * Delete the Process group
     *
     * @param pge
     * @return
     */
    private ProcessGroupEntity deleteProcessGroup(ProcessGroupEntity pge) {
        return processGroup.deleteProcessGroup(pge, EntityState.DELETE.getState());
    }
}
