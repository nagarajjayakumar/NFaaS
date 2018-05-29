package com.hortonworks.faas.nfaas.core.helper;

import com.hortonworks.faas.nfaas.core.FlowFileQueue;
import com.hortonworks.faas.nfaas.core.ProcessGroupFlow;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
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
