package com.hortonworks.faas.nfaas.controller;

import com.hortonworks.faas.nfaas.dto.ActiveObject;
import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;
import com.hortonworks.faas.nfaas.orm.ActiveObjectDetailRepository;
import com.hortonworks.faas.nfaas.orm.ActiveObjectRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

/**
 * A class to interactions with the MySQL database using the ActiveObjectRepository class.
 *
 * @author njayakumar
 */
@Controller
public class MetaDbController {

    /**
     * /getaod  --> Return the Active Object detail
     *
     */
    @CrossOrigin
    @PreAuthorize("#oauth2.hasScope('read')")
    @RequestMapping(value = "/mdb/getaod", produces = "application/json")
    public @ResponseBody
    List<ActiveObjectDetail> getActiveObjectDetail(String namespace, String package_id, String db_object_name) {
        ActiveObject activeObject;
        List<ActiveObjectDetail> activeObjectDetail;
        try {
            activeObject = activeObjectRepository.findByNamespaceAndPackageIdAndDbObjectName(namespace,package_id,db_object_name);
            activeObjectDetail = activeObjectDetailRepository.findAllByHaoid(activeObject.getId());

        }
        catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
        return activeObjectDetail;
    }

    // ------------------------
    // PRIVATE FIELDS
    // ------------------------

    @Autowired
    private ActiveObjectRepository activeObjectRepository;

    @Autowired
    private ActiveObjectDetailRepository activeObjectDetailRepository;
}
