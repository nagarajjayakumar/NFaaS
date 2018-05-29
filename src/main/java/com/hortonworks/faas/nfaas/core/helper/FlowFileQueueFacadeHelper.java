package com.hortonworks.faas.nfaas.core.helper;

import com.hortonworks.faas.nfaas.core.FlowFileQueue;
import com.hortonworks.faas.nfaas.core.ProcessGroupFlow;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Set;

public class FlowFileQueueFacadeHelper {

    public static final Logger logger = LoggerFactory.getLogger(FlowFileQueueFacadeHelper.class);

    @Autowired
    ProcessGroupFlow processGroupFlow;

    @Autowired
    FlowFileQueue flowFileQueue;

    /**
     * This is the method to delete teh root proces group queu content
     *
     * @param rootPgId
     */
    public void deleteRootProcessGroupQueueContentIfAny(String rootPgId) {
        ProcessGroupFlowEntity pgfe = null;
        pgfe = processGroupFlow.getLatestProcessGroupFlowEntity(rootPgId);

        Set<ConnectionEntity> connections = pgfe.getProcessGroupFlow().getFlow().getConnections();

        requestAndDeleteQueueContent(connections);


    }

    /**
     * Delete the queue content for the process group entity
     *
     * @param pge
     */
    private void deleteTheQueueContent(ProcessGroupEntity pge) {
        if (flowFileQueue.isDeleteQueueContent() == false)
            throw new RuntimeException("Queues Are Not Empty.. Please flush the queus manually before deletion...");

        ProcessGroupFlowEntity pgfe = processGroupFlow.getLatestProcessGroupFlowEntity(pge.getId());
        Set<ProcessGroupEntity> processGroups = pgfe.getProcessGroupFlow().getFlow().getProcessGroups();

        int queuedCount = 0;

        for (ProcessGroupEntity processGroupEntity : processGroups) {
            queuedCount = Integer.parseInt(processGroupEntity.getStatus().getAggregateSnapshot().getQueuedCount().replaceAll(",", ""));

            if (queuedCount > 0) {
                deleteTheQueueContent(processGroupEntity);
            }
        }

        Set<ConnectionEntity> connections = pgfe.getProcessGroupFlow().getFlow().getConnections();

        requestAndDeleteQueueContent(connections);

    }

    private void requestAndDeleteQueueContent(Set<ConnectionEntity> connections) {
        int queuedCountInConnections = 0;
        DropRequestEntity dre = null;
        for (ConnectionEntity connection : connections) {
            queuedCountInConnections = Integer.parseInt(connection.getStatus().getAggregateSnapshot().getQueuedCount().replaceAll(",", ""));
            if (queuedCountInConnections > 0) {
                dre = flowFileQueue.placeRequestForDeletion(connection);
                dre = flowFileQueue.deleteTheQueueContent(dre);
            }
        }
    }

}
