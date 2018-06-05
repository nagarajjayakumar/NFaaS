package com.hortonworks.faas.nfaas.controller;

import com.hortonworks.faas.nfaas.dto.ActiveObject;
import com.hortonworks.faas.nfaas.orm.ActiveObjectRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * A class to interactions with the MySQL database using the ActiveObjectRepo class.
 *
 * @author njayakumar
 */
@Controller
public class ActiveObjectController {

    /**
     * /getaod  --> Return the Active Object detail
     *
     */
    @CrossOrigin
    @PreAuthorize("#oauth2.hasScope('read')")
    @RequestMapping(value = "/mdb/getaod", produces = "application/json")
    public @ResponseBody
    ActiveObject getActiveObjectDetail(String namespace, String package_id, String db_object_name) {
        ActiveObject activeObject;
        try {
            activeObject = activeObjectRepo.findByNamespaceAndPackageIdAndDbObjectName(namespace,package_id,db_object_name);

        }
        catch (Exception ex) {
            return null;
        }
        return activeObject;
    }

    // ------------------------
    // PRIVATE FIELDS
    // ------------------------

    @Autowired
    private ActiveObjectRepo activeObjectRepo;
}
