package com.hortonworks.faas.nfaas.core.helper;

import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.LinkedHashSet;
import java.util.Set;

public class RemoteProcessGroupFacadeHelper {

    private static final Logger logger = LoggerFactory.getLogger(RemoteProcessGroupFacadeHelper.class);


    @Autowired
    TemplateFacadeHelper templateFacadeHelper;

    /**
     * This is the method which is used to get all the remote process group from
     * the Pgfe
     *
     * @param pgfe
     * @param remoteProcessGroupsFromTemplate
     * @return
     */
    @SuppressWarnings("unused")
    private Set<RemoteProcessGroupEntity> getRemoteProcessGroupEntityForUndeploy(ProcessGroupFlowEntity pgfe,
                                                                                 Set<RemoteProcessGroupDTO> remoteProcessGroupsFromTemplate) {

        Set<RemoteProcessGroupEntity> resultRemotePG = new LinkedHashSet<>();
        Set<RemoteProcessGroupEntity> allRemoteProcessGroups = pgfe.getProcessGroupFlow().getFlow()
                .getRemoteProcessGroups();

        Set<String> remoteProcessorGroupNameFromTemplate = templateFacadeHelper.getAllRemoteProcessorGroupNameFromTemplate(
                remoteProcessGroupsFromTemplate);

        for (RemoteProcessGroupEntity rpge : allRemoteProcessGroups) {
            if (remoteProcessorGroupNameFromTemplate.contains(rpge.getComponent().getName())) {
                resultRemotePG.add(rpge);
            }

        }
        return resultRemotePG;
    }
}
