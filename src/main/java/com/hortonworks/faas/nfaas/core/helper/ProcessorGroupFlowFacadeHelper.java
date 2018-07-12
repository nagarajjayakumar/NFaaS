package com.hortonworks.faas.nfaas.core.helper;

import com.hortonworks.faas.nfaas.config.EntityState;
import org.apache.nifi.web.api.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Configuration
public class ProcessorGroupFlowFacadeHelper extends  BaseFacadeHelper{

    public static final Logger logger = LoggerFactory.getLogger(ProcessGroupFacadeHelper.class);

    @Autowired
    ProcessGroupFacadeHelper processGroupFacadeHelper;
    /**
     * Call the NIFI rest api to stop the process group
     *
     * @param processGroupFlowEntity
     * @param state
     */
    public void stopProcessGroupComponents(ProcessGroupFlowEntity processGroupFlowEntity, String state) {
        processGroupFlow.startOrStopProcessGroupComponents(processGroupFlowEntity, state);

    }

    /**
     * Call the NIFI rest api to stop the process group
     *
     * @param processGroupFlowEntity
     * @param state
     */
    public void startProcessGroupComponents(ProcessGroupFlowEntity processGroupFlowEntity, String state) {
        processGroupFlow.startOrStopProcessGroupComponents(processGroupFlowEntity, state);

    }


    /**
     * Method to stop all the process group components
     *
     * @param processGroupFlowEntity
     * @return
     */
    public ProcessGroupFlowEntity stopProcessGroupComponents(ProcessGroupFlowEntity processGroupFlowEntity,
                                                              ProcessGroupEntity processorGroup,
                                                              String pgId) {
        stopProcessGroupComponents(processGroupFlowEntity, EntityState.STOPPED.getState());
        checkProcessGroupComponentStatus(processGroupFlowEntity, EntityState.STOPPED.getState(), pgId);
        ProcessGroupFlowEntity pge = processGroupFlow.getLatestProcessGroupFlowEntity(
                processGroupFlowEntity.getProcessGroupFlow().getId());
        return pge;
    }



    public void checkInternalProcessGroupStatus(ProcessGroupFlowEntity processGroupFlowEntity, String state) {
        int count = 0;
        int innerCount = 0;
        ProcessGroupFlowEntity pgfe = null;
        //ProcessGroupFlowEntity currentPgfe = null;


        while (true && count < WAIT_IN_SEC) {
            pgfe = processGroupFlow.getLatestProcessGroupFlowEntity(processGroupFlowEntity.getProcessGroupFlow().getId());

            Set<ProcessGroupEntity> processGroups = pgfe.getProcessGroupFlow().getFlow().getProcessGroups();

            int queuedCount = 0;
            for (ProcessGroupEntity processGroupEntity : processGroups) {

				/*if(! processGroupEntity.getComponent().getContents().getProcessGroups().isEmpty()){
					currentPgfe = getLatestProcessGroupFlowEntity(processGroupEntity.getId());
					checkInternalProcessGroupStatus(currentPgfe, state);
				}*/
                /*
                 * Stop only the necessary process groups for the given process
                 * group ID
                 */
                if (processGroupEntity.getComponent().getParentGroupId()
                        .equalsIgnoreCase(processGroupFlowEntity.getProcessGroupFlow().getId())) {

                    if (state.equalsIgnoreCase(EntityState.STOPPED.getState())) {
                        queuedCount = Integer
                                .parseInt(processGroupEntity.getStatus().getAggregateSnapshot().getQueuedCount().replaceAll(",", ""));
                        // Check for the Runing count
                        if (processGroupEntity.getRunningCount() > 0) {
                            break;
                        }
                        // Check for the queue content
                        if (queuedCount > 0) {
                            flowFileQueue.deleteTheQueueContent(processGroupEntity);
                            break;
                        }

                    }

                    if (state.equalsIgnoreCase(EntityState.RUNNING.getState())
                            && processGroupEntity.getStoppedCount() > 0) {
                        break;
                    }
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
            count++;
            innerCount = 0;
        }
    }

    /**
     * Check the Process Group Component Status
     *
     * @param processGroupFlowEntity
     * @param state
     */
    public void checkProcessGroupComponentStatus(ProcessGroupFlowEntity processGroupFlowEntity, String state,
                                                 String pgId) {
        checkInternalProcessGroupStatus(processGroupFlowEntity, state);

        ProcessGroupEntity pge = processGroup.getLatestProcessGroupEntity(pgId);

        processGroupFacadeHelper.checkParentProcessGroupStatus(pge, state);
    }



    public ProcessGroupEntity importProcessGroupFromRegistry(ProcessGroupFlowEntity pgfe,
                                                             ProcessGroupEntity pge,
                                                             String flowName,
                                                             int version_num,
                                                             String registryName) {
        String clientId = processGroupFlow.getClientId();

        String prod_registry_id = getRegistryId(registryName);
        BucketEntity  prod_bucket = getBucket(prod_registry_id);
        VersionedFlowEntity prod_flow = getFlowFromRegistry(prod_registry_id,prod_bucket,flowName);

        // I face some weird issue .. need to check on that
        //VersionedFlowSnapshotMetadataEntity prod_flow_version = getFlowSnapShotFromRegistryAndBucketAndFlow(prod_registry_id,prod_bucket,prod_flow, version_num);

        ProcessGroupEntity ret_pge = processGroup.createProcessGroup(pge.getId(),
                                        clientId,
                                        flowName,
                                        prod_registry_id,
                                        prod_bucket.getId(),
                                        prod_flow.getVersionedFlow().getFlowId(),
                                        version_num);

        return ret_pge;

    }

    /**
     * This is the method to save the process group in the flow registry
     * @param pgfe
     * @param pge
     * @param flowName
     * @param version_num
     * @param registryName
     * @return
     */
    public VersionControlInformationEntity saveProcessGroupWithId(ProcessGroupFlowEntity pgfe,
                                                     ProcessGroupEntity pge,
                                                     String flowName,
                                                     long version_num,
                                                     String registryName,
                                                     String comment) {

        String clientId = processGroupFlow.getClientId();

        String prod_registry_id = getRegistryId(registryName);
        BucketEntity  prod_bucket = getBucket(prod_registry_id);

        VersionControlInformationEntity versionCtrlInfoEntity =
                                    version.saveProcessGroupById(pge.getId(),
                                                                 clientId,
                                                                 flowName,
                                                                 prod_registry_id,  prod_bucket.getId(), version_num, comment );



        return versionCtrlInfoEntity;
    }

    public VersionControlInformationEntity updateProcessGroupWithId(ProcessGroupFlowEntity pgfe,
                                                                  ProcessGroupEntity pge,
                                                                  String flowName,
                                                                  long version_num,
                                                                  String registryName,
                                                                  String comment) {

        String clientId = processGroupFlow.getClientId();

        String prod_registry_id = getRegistryId(registryName);
        BucketEntity  prod_bucket = getBucket(prod_registry_id);

        VersionControlInformationEntity versionCtrlInfoEntity =
                version.updateProcessGroupById(pge,
                        clientId,
                        flowName,
                        prod_registry_id,  prod_bucket.getId(), version_num, comment );



        return versionCtrlInfoEntity;
    }

    public VersionControlInformationEntity stopVersionControlForPge(ProcessGroupEntity pge){
        VersionControlInformationEntity versionCtrlInfoEntity = version.stopVersionControlForPge(pge);

        return versionCtrlInfoEntity;
    }

    // this is the method to get the flow version
    private VersionedFlowSnapshotMetadataEntity getFlowSnapShotFromRegistryAndBucketAndFlow(String prod_registry_id,
                                                                                            BucketEntity prod_bucket,
                                                                                            VersionedFlowEntity prod_flow,
                                                                                            int version_num) {

        VersionedFlowSnapshotMetadataSetEntity versionedFlows =
                processGroupFlow.getFlowSnapShotFromRegistryAndBucketAndFlow(prod_registry_id,
                                                                             prod_bucket.getId(),
                                                                             prod_flow.getVersionedFlow().getFlowId());

        VersionedFlowSnapshotMetadataEntity versionedFlow = versionedFlows.getVersionedFlowSnapshotMetadataSet().iterator().next();

        for(VersionedFlowSnapshotMetadataEntity vsme : versionedFlows.getVersionedFlowSnapshotMetadataSet()){
            if(version_num == vsme.getVersionedFlowSnapshotMetadata().getVersion()){
                versionedFlow = vsme;
                break;
            }
        }

        return versionedFlow;
    }

    // this is the method to get the flow
    private VersionedFlowEntity getFlowFromRegistry(String prod_registry_id, BucketEntity prod_bucket, String flowName) {
        VersionedFlowsEntity versionedFlows = processGroupFlow.getAllFlowsFromRegistryAndBucket(prod_registry_id,prod_bucket.getId());
        VersionedFlowEntity versionedFlow = versionedFlows.getVersionedFlows().iterator().next();

        for(VersionedFlowEntity vfe : versionedFlows.getVersionedFlows()){
            if(flowName.equalsIgnoreCase(vfe.getVersionedFlow().getFlowName().toLowerCase())){
                versionedFlow = vfe;
                break;
            }
        }
        return versionedFlow;
    }

    // this is the method to get the bucket from the prod_registry
    private BucketEntity getBucket(String prod_registry_id) {
        BucketsEntity buckets = processGroupFlow.getBucket(prod_registry_id);
        BucketEntity bucket = buckets.getBuckets().iterator().next();

        for(BucketEntity be : buckets.getBuckets()){
            if("prod".equalsIgnoreCase(be.getBucket().getName().toLowerCase())){
                bucket = be;
                break;
            }
        }

        return bucket;
    }

    // this is the method to get the registry
    private String getRegistryId(String registryName) {

        return processGroupFlow.getAvailableRegistry(registryName);

//        RegistryClientsEntity registries = (RegistryClientsEntity) processGroupFlow.getAvailableRegistry();
//        RegistryClientEntity registry = registries.getRegistries().iterator().next();
//
//        for (RegistryClientEntity rce : registries.getRegistries()){
//            if ("prod_registry".equalsIgnoreCase(rce.getComponent().getName().toLowerCase())){
//                registry = rce;
//                break;
//            }
//        }
//        return registry;

    }
}
