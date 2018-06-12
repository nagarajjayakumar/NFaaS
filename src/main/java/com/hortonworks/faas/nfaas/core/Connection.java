/**
 * @author Adam Michalsky
 *
 */

package com.hortonworks.faas.nfaas.core;


import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import javax.persistence.Entity;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class Connection {

    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    Environment env;

    private String trasnsportMode = "http";
    private boolean nifiSecuredCluster = false;
    private String nifiServerHostnameAndPort = "localhost:8080";

    @Autowired
    Security security;

    @Autowired
    ProcessGroupFlow processGroupFlow;

    @Autowired
    PortEntity portEntity;

    @Autowired
    ConnectionEntity connectionEntity;

    @Autowired
    ProcessorEntity processorEntity;

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
    public ConnectionEntity createConnectionEntity(Entity src, Entity dest, String clientId)
    {
        Map<String, String> params = new HashMap<String, String>();
        String src_pgId = this.getEntityParentGroupId(src);
        String dest_pgId = this.getEntityParentGroupId(dest);
        String srcId = this.getEntityId(src);
        String destId = this.getEntityId(dest);

        /*
        Create connection entity and connectable DTOs for source and destinations
         */
        ConnectionEntity connection_entity = new ConnectionEntity();
        ConnectableDTO source = new ConnectableDTO();
        ConnectableDTO destination = new ConnectableDTO();
        ConnectionDTO component = new ConnectionDTO();


        /*
        Assign the source and destination parent Ids and entity Ids to the connectable DTOs
         */
        source.setId(srcId);
        source.setGroupId(src_pgId);
        destination.setId(destId);
        destination.setGroupId(dest_pgId);

        component.setDestination(destination);
        component.setSource(source);

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



        String theUrl = trasnsportMode + "://" + nifiServerHostnameAndPort + "/nifi-api/process-groups/"+ src_pgId + "/connections/";
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
        String connId = connectionEntity.getId();

        // https://"+nifiServerHostnameAndPort+"/nifi-api/process-groups/a57d7d2a-86bd-4b43-357a-34abb1bd85d6?version=0&clientId=deaebc77-015b-1000-31ea-162516e98255
        String version = String.valueOf(commonService.getClientIdAndVersion(connectionEntity).getVersion());
        String clientId = String.valueOf(commonService.getClientIdAndVersion(connectionEntity).getClientId());

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
     * Helper function that will return the id of the given entity
     * @param entity entity that will be interrogated for parent Id
     * @return id of the entity
     */
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

}

