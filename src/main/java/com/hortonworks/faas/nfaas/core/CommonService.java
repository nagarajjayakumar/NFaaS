package com.hortonworks.faas.nfaas.core;

import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;

import java.util.Random;

public class CommonService {

    /**
     * Update the last Modified for the entity.
     *
     * @param entity
     * @return
     */
    public RevisionDTO updateLastModified(ComponentEntity entity) {
        RevisionDTO revision = getClientIdAndVersion(entity);
        revision.setLastModifier(getUser());
        return revision;
    }

    /**
     * This is the method to get the client ID and version for the Component
     * Entity
     *
     * @param entity
     * @return
     */
    public RevisionDTO getClientIdAndVersion(ComponentEntity entity) {
        RevisionDTO revision = new RevisionDTO();

        if (null != entity) {
            return entity.getRevision();
        }

        return revision;
    }

    public String getUser() {
        // TODO Auto-generated method stub
        return "hdfm";
    }

    public Double getXaxis() {
        double rangeMin = 1500;
        double rangeMax = 2000;
        Random r = new Random();
        double randomValue = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
        return randomValue;
    }

    public Double getYaxis() {
        double rangeMin = 1000;
        double rangeMax = 1500;
        Random r = new Random();
        double randomValue = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
        return randomValue;
    }

}
