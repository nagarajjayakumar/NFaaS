package com.hortonworks.faas.nfaas.core.helper;

import com.hortonworks.faas.nfaas.core.ControllerService;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ControllerServiceFacadeHelper {

    private static final Logger logger = LoggerFactory.getLogger(ControllerServiceFacadeHelper.class);

    @Autowired
    ControllerService controllerService;
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

}
