package com.hortonworks.faas.nfaas.core.helper;

import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashSet;
import java.util.Set;

@Configuration
public class ConnectionFacadeHelper extends BaseFacadeHelper {

    private static final Logger logger = LoggerFactory.getLogger(InputPortFacadeHelper.class);

    /**
     * This is the method to get all the input port entity from the pgfe
     *
     * @param pgfe
     * @param connectionFromTemplate
     * @return
     */
    public Set<ConnectionEntity> getConnectionEntityForUndeploy(ProcessGroupFlowEntity pgfe,
                                                          Set<ConnectionDTO> connectionFromTemplate) {

        Set<ConnectionEntity> resultConnections = new LinkedHashSet<>();
        Set<ConnectionEntity> allConnections = pgfe.getProcessGroupFlow().getFlow().getConnections();

        Set<String> inputPortsNameFromTemplate = templateFacadeHelper.getAllConnectionsFromTemplate(connectionFromTemplate);

        for (ConnectionEntity conn : allConnections) {
            if (inputPortsNameFromTemplate.contains(conn.getComponent().getName())) {
                resultConnections.add(conn);
            }

        }
        return resultConnections;
    }

    /**
     * This is the method which is used to delete the Connection Entity ,,,,
     *
     * @param connectionEntity
     * @return
     */
    public void deleteConnectionEntity(ConnectionEntity connectionEntity) {
        ConnectionEntity conn = connection.getLatestConnectionEntity(connectionEntity);
        conn = connection.deleteConnectionEntity(conn);
        logger.info(conn.toString());
    }

    /**
     * Searches for an connection in a process group flow entity
     *
     * @param pgfe ProcessGroupFlowEntity to be searched
     * @param connectionName Name of connection
     * @return ConnectionEntity found with the provided name - null if no port is found.
     */
    public ConnectionEntity getConnectionEntityByName(ProcessGroupFlowEntity pgfe,
                                          String connectionName) {

        ConnectionEntity resultConn = null;
        Set<ConnectionEntity> allConnections = pgfe.getProcessGroupFlow().getFlow().getConnections();

        for (ConnectionEntity conn : allConnections) {
            if (connectionName.equalsIgnoreCase(conn.getComponent().getName())) {
                resultConn = conn;
                break;
            }
        }

        return resultConn;

    }


}
