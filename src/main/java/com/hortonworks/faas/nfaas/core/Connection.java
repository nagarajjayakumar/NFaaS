/**
 * @author Adam Michalsky
 *
 */

package com.hortonworks.faas.nfaas.core;


import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class Connection {

    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    Environment env;

    private String trasnsportMode = "http";
    private boolean nifiSecuredCluster = false;
    private String nifiServerHostnameAndPort = "localhost:8080";
    private String BACKPRESSURE_DATA_SIZE_THRESHOLD_DEFAULT = "1 GB";
    private long BACKPRESSURE_OBJECT_THRESHOLD_DEFAULT = 10000;
    private String FLOW_FILE_EXPIRATION_DEFAULT = "0 sec";

    @Autowired
    Security security;

    @Autowired
    ProcessGroupFlow processGroupFlow;

    @Autowired
    FlowFileQueue flowFileQueue;

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    CommonService commonService;

    @Autowired
    Connection(Environment env) {
        logger.info("Intialized Connection !!! ");
        this.env = env;
        this.trasnsportMode = env.getProperty("nifi.trasnsportMode");
        this.nifiSecuredCluster = Boolean.parseBoolean(env.getProperty("nifi.securedCluster"));
        this.nifiServerHostnameAndPort = env.getProperty("nifi.hostnameAndPort");
    }

    /**
     *  Creates a connection between a source entity and destination entity
     * @param src source entity for the connection
     * @param dest destination entity for the connection
     * @param clientId clientId to be used while creating the connection entity
     * @return the connection entity created.
     */
    public ConnectionEntity createConnectionEntityPorts(PortEntity src, PortEntity dest, String clientId, String connectionName)
    {
        Map<String, String> params = new HashMap<String, String>();
        Map<String, String> connectionConfig = new HashMap<String, String>();


        connectionConfig.put("src_pgId", src.getComponent().getParentGroupId());
        connectionConfig.put("dest_pgId", dest.getComponent().getParentGroupId());
        connectionConfig.put("srcId", src.getComponent().getId());
        connectionConfig.put("destId", dest.getComponent().getId());
        connectionConfig.put("srcType", src.getComponent().getType());
        connectionConfig.put("destType", dest.getComponent().getType());
        connectionConfig.put("name", connectionName);


        ConnectionDTO component = initializeComponent(connectionConfig);
        ConnectionEntity connection_entity = new ConnectionEntity();

        component.setName(connectionName);
        connection_entity.setComponent(component);

        /*
          Very critical to set the client Id and the inital version
          Otherwise the Httprequest will turn to a bad request.
         */
        RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId);
        revision.setVersion(0l);
        connection_entity.setRevision(revision);


        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<ConnectionEntity> requestEntity = new HttpEntity<ConnectionEntity>(connection_entity,requestHeaders);



        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/process-groups/"+ connectionConfig.get("src_pgId") + "/connections/";
        HttpEntity<ConnectionEntity> response = restTemplate.exchange(theUrl, HttpMethod.POST, requestEntity, ConnectionEntity.class,
                params);
        return response.getBody();
    }

    /**
     * Deletes the provided connection entity
     * @param connection connection entity to be delete
     * @return response of delete call
     */
    public ConnectionEntity deleteConnectionEntity(ConnectionEntity connection)
    {
        String connId = connection.getId();

        // https://"+nifiServerHostnameAndPort+"/nifi-api/process-groups/a57d7d2a-86bd-4b43-357a-34abb1bd85d6?version=0&clientId=deaebc77-015b-1000-31ea-162516e98255
        String version = String.valueOf(commonService.getClientIdAndVersion(connection).getVersion());
        String clientId = String.valueOf(commonService.getClientIdAndVersion(connection).getClientId());

        DropRequestEntity dre = flowFileQueue.placeRequestForDeletion(connection);
        flowFileQueue.deleteTheQueueContent(dre);

        final String uri = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/connections/" + connId + "?version="
                + version + "&clientId=" + clientId;

        Map<String, String> params = new HashMap<String, String>();

        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

        ConnectionEntity resp = null;
        HttpEntity<ConnectionEntity> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity, ConnectionEntity.class,
                params);

        resp = response.getBody();

        logger.debug(resp.toString());
        return resp;
    }

    /**
     * This is the method to get the latest Connection Entity
     *
     * @param connectionEntity
     * @return
     */
    public ConnectionEntity getLatestConnectionEntity(ConnectionEntity connectionEntity) {
        Map<String, String> params = new HashMap<String, String>();
        HttpHeaders requestHeaders = security.getAuthorizationHeader();
        HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/connections/" + connectionEntity.getId() + "/";
        HttpEntity<ConnectionEntity> response = restTemplate.exchange(theUrl, HttpMethod.GET, requestEntity, ConnectionEntity.class,
                params);
        return response.getBody();
    }

    /*
    /**
     * Helper function that will return the id of the given entity
     * @param entity entity that will be interrogated for parent Id
     * @return id of the entity

    private String getEntityId(Entity entity)
    {
        String entity_class = entity.getClass().toString();
        String resultId;

        if(entity_class == portEntity.getClass().toString()) //Entity is a Port Entity
        {
            PortEntity port = (PortEntity) entity;
            resultId = port.getId();
        }else if(entity_class == processorEntity.getClass().toString()) //Entity is a Processor Entity
        {
            ProcessorEntity processor = (ProcessorEntity) entity;
            resultId = processor.getId();
        }else{
            resultId = null;
        }

        return resultId;
    }

    /**
     *  Helper function that will return the id of the parent group for a given entity
     * @param entity entity that will be interrogated for parent Id
     * @return id of Parent Group
      */
    /*
    private String getEntityParentGroupId(Entity entity)
    {
        String entity_class = entity.getClass().toString();
        String resultId;

        if(entity_class == portEntity.getClass().toString()) //Entity is a Port Entity
        {
            PortEntity port = (PortEntity) entity;
            resultId = port.getComponent().getParentGroupId();
        }else if(entity_class == processorEntity.getClass().toString()) //Entity is a Processor Entity
        {
            ProcessorEntity processor = (ProcessorEntity) entity;
            resultId = processor.getComponent().getParentGroupId();
        }else{
            resultId = null;
        }

        return resultId;
    }
    */

    private ConnectionDTO initializeComponent(Map<String, String> connectionConfig)
    {
             /*
        Create connection entity and connectable DTOs for source and destinations
         */
        ConnectableDTO source = new ConnectableDTO();
        ConnectableDTO destination = new ConnectableDTO();
        ConnectionDTO component = new ConnectionDTO();


        /*
        Assign the source and destination parent Ids and entity Ids to the connectable DTOs
         */
        source.setId(connectionConfig.get("srcId"));
        source.setGroupId(connectionConfig.get("src_pgId"));
        source.setType(connectionConfig.get("srcType"));
        destination.setId(connectionConfig.get("destId"));
        destination.setGroupId(connectionConfig.get("dest_pgId"));
        destination.setType(connectionConfig.get("destType"));


        component.setDestination(destination);
        component.setSource(source);
        component.setName(connectionConfig.get("name"));
        component.setBackPressureDataSizeThreshold(BACKPRESSURE_DATA_SIZE_THRESHOLD_DEFAULT);
        component.setBackPressureObjectThreshold(BACKPRESSURE_OBJECT_THRESHOLD_DEFAULT);
        component.setFlowFileExpiration(FLOW_FILE_EXPIRATION_DEFAULT);

        return component;
    }

}

