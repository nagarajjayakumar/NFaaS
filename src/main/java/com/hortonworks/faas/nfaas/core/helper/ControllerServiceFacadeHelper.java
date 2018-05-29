package com.hortonworks.faas.nfaas.core.helper;

import com.hortonworks.faas.nfaas.config.EntityState;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Configuration
public class ControllerServiceFacadeHelper extends BaseFacadeHelper {

    private static final Logger logger = LoggerFactory.getLogger(ControllerServiceFacadeHelper.class);

    /**
     * Check for the reference component. check for the state else sleep for 10
     * sec
     *
     * @param controllerServiceEntity
     * @param state
     */
    private void checkReferenceComponentStatus(ControllerServiceEntity controllerServiceEntity, String state) {
        int count = 0;
        int innerCount = 0;
        ControllerServiceEntity cse = null;

        while (true && count < WAIT_IN_SEC) {
            cse = controllerService.getLatestControllerServiceEntity(controllerServiceEntity);

            Set<ControllerServiceReferencingComponentEntity> referencingComponents = cse.getComponent()
                    .getReferencingComponents();

            for (ControllerServiceReferencingComponentEntity csrRefComp : referencingComponents) {

                if (!state.equalsIgnoreCase(csrRefComp.getComponent().getState())) {
                    break;
                }
                innerCount++;
            }

            if (referencingComponents.size() == innerCount) {
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
     * Stop and Un deploy the controller Services.
     *
     * @param controllerServicesEntity
     */
    private void stopAndUnDeployControllerServices(ControllerServicesEntity controllerServicesEntity) {

        Set<ControllerServiceEntity> controllerServicesEntities = controllerServicesEntity.getControllerServices();

        ControllerServiceEntity cse = null;

        for (ControllerServiceEntity controllerServiceEntity : controllerServicesEntities) {
            logger.info("stopAndUnDeployControllerServices Starts for --> "
                    + controllerServiceEntity.getComponent().getName());
            cse = stopRefrencingComponents(controllerServiceEntity);
            cse = disableControllerService(cse);
            cse = deleteControllerService(cse);
            logger.info("stopAndUnDeployControllerServices Ends for --> "
                    + controllerServiceEntity.getComponent().getName());

        }

    }

    /**
     * Method is used to enable the controller services
     *
     * @param controllerServicesEntity
     * @return
     */
    private void enableAllControllerServices(ControllerServicesEntity controllerServicesEntity) {
        Set<ControllerServiceEntity> controllerServicesEntities = controllerServicesEntity.getControllerServices();
        ControllerServiceEntity cse = null;
        for (ControllerServiceEntity controllerServiceEntity : controllerServicesEntities) {
            if (EntityState.INVALID.getState().equalsIgnoreCase(controllerServiceEntity.getComponent().getState())) {
                logger.error("Controller Services is in invalid state.. Please validate --> "
                        + controllerServiceEntity.getComponent().getName());
                continue;
            }
            logger.info("Controller Services Enable Starts --> " + controllerServiceEntity.getComponent().getName());
            cse = enableControllerService(controllerServiceEntity);
            logger.debug(cse.toString());
            logger.info("Controller Services Enable Ends   --> " + controllerServiceEntity.getComponent().getName());
        }
    }

    /**
     * Method is used to enable the controller services
     *
     * @param controllerServicesEntity
     * @return
     */
    public void disableAllControllerServices(ControllerServicesEntity controllerServicesEntity) {
        Set<ControllerServiceEntity> controllerServicesEntities = controllerServicesEntity.getControllerServices();
        ControllerServiceEntity cse = null;
        for (ControllerServiceEntity controllerServiceEntity : controllerServicesEntities) {
            logger.info(
                    "disableAllControllerServices Starts for --> " + controllerServiceEntity.getComponent().getName());
            cse = stopRefrencingComponents(controllerServiceEntity);
            cse = disableControllerService(cse);
            logger.info(
                    "disableAllControllerServices Ends for --> " + controllerServiceEntity.getComponent().getName());
        }
    }

    /**
     * Method is used to enable the controller services
     *
     * @param controllerServicesEntity
     * @return
     */
    public void deleteAllControllerServices(ControllerServicesEntity controllerServicesEntity) {
        Set<ControllerServiceEntity> controllerServicesEntities = controllerServicesEntity.getControllerServices();
        ControllerServiceEntity cse = null;
        for (ControllerServiceEntity controllerServiceEntity : controllerServicesEntities) {
            logger.info(
                    "deleteAllControllerServices Starts for --> " + controllerServiceEntity.getComponent().getName());
            cse = deleteControllerService(controllerServiceEntity);
            logger.info("deleteAllControllerServices Ends for --> " + controllerServiceEntity.getComponent().getName()
                    + cse.toString());
        }
    }


    /**
     * Stop the referencing component of the controller services
     *
     * @param controllerServiceEntity
     * @return
     */
    public ControllerServiceEntity stopRefrencingComponents(ControllerServiceEntity controllerServiceEntity) {

        logger.info("Stopping Controller Refrence Component Starts --> "
                + controllerServiceEntity.getComponent().getName());
        controllerService.stopReferencingComponents(controllerServiceEntity, EntityState.STOPPED.getState());

        checkReferenceComponentStatus(controllerServiceEntity, EntityState.STOPPED.getState());
        ControllerServiceEntity cse = controllerService.getLatestControllerServiceEntity(controllerServiceEntity);
        logger.info(
                "Stopping Controller Refrence Component Ends --> " + controllerServiceEntity.getComponent().getName());
        return cse;
    }

    /**
     * Disable the controller Service.
     *
     * @param cse
     * @return
     */
    public ControllerServiceEntity disableControllerService(ControllerServiceEntity cse) {

        logger.info("Disable Controller Service Starts --> " + cse.getComponent().getName());
        controllerService.disableControllerServiceUsingRef(cse, EntityState.DISABLED.getState());

        // No need to check the status now ..
        //checkControllerServiceStatus(cse, EntityState.DISABLED.getState());
        cse = controllerService.getLatestControllerServiceEntity(cse);

        disableControllerService(cse, EntityState.DISABLED.getState());

        checkControllerServiceStatus(cse, EntityState.DISABLED.getState());
        logger.info("Disable Controller Service Ends --> " + cse.getComponent().getName());
        return controllerService.getLatestControllerServiceEntity(cse);
    }

    /**
     * Disable the controller Service.
     *
     * @param cse
     * @return
     */
    public ControllerServiceEntity enableControllerService(ControllerServiceEntity cse) {

        logger.info("Enable Controller Service Starts --> " + cse.getComponent().getName());
        cse = controllerService.getLatestControllerServiceEntity(cse);
        enableControllerService(cse, EntityState.ENABLED.getState());

        checkControllerServiceStatus(cse, EntityState.ENABLED.getState());
        logger.info("Enable Controller Service Ends --> " + cse.getComponent().getName());
        return controllerService.getLatestControllerServiceEntity(cse);
    }

    /**
     * Delete teh controller Service
     *
     * @param controllerServiceEntity
     * @return
     */
    public ControllerServiceEntity deleteControllerService(ControllerServiceEntity controllerServiceEntity) {
        return controllerService.deleteControllerService(controllerServiceEntity, EntityState.DELETE.getState());
    }


    /**
     * This is the method to disable the controller service
     *
     * @param controllerServiceEntity
     * @param state
     */

    public void disableControllerService(ControllerServiceEntity controllerServiceEntity, String state) {
        controllerService.changeControllServiceState(controllerServiceEntity, state);

    }

    /**
     * This is the method to Enable the controller service
     *
     * @param controllerServiceEntity
     * @param state
     */

    public void enableControllerService(ControllerServiceEntity controllerServiceEntity, String state) {
        controllerService.changeControllServiceState(controllerServiceEntity, state);

    }


    /**
     * Check the controller service entity status
     *
     * @param controllerServiceEntity
     * @param state
     */
    public void checkControllerServiceStatus(ControllerServiceEntity controllerServiceEntity, String state) {
        int count = 0;

        ControllerServiceEntity cse = null;

        while (true && count < WAIT_IN_SEC) {
            cse = controllerService.getLatestControllerServiceEntity(controllerServiceEntity);

            if (state.equalsIgnoreCase(cse.getComponent().getState())) {
                break;
            }

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {

            }

            count++;
        }

    }


}
