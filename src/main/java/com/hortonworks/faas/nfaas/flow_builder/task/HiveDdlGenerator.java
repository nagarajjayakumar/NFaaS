package com.hortonworks.faas.nfaas.flow_builder.task;

import com.hortonworks.faas.nfaas.controller.MetaDbController;
import com.hortonworks.faas.nfaas.dto.ActiveObject;
import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;
import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;
import com.hortonworks.faas.nfaas.orm.ActiveObjectDetailRepository;
import com.hortonworks.faas.nfaas.orm.ActiveObjectRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

public class HiveDdlGenerator implements  Task{

    private static final Logger logger = LoggerFactory.getLogger(HiveDdlGenerator.class);
    public String task = "hive_ddl_generator";

    private FlowBuilderOptions _fbo = null;

    @Autowired
    private ActiveObjectRepository activeObjectRepository;

    @Autowired
    private ActiveObjectDetailRepository activeObjectDetailRepository;

    @Override
    public void doWork(FlowBuilderOptions fbo) {
        logger.info(String.format("started %s !! ",task));
        this._fbo = fbo;
        ActiveObject activeObject = this.getActiveObject();
        List<ActiveObjectDetail> aod = this.getActiveObjectDetail();
        logger.info(String.format("ended %s !! ",task));
    }

    private List<ActiveObjectDetail> getActiveObjectDetail() {
        ActiveObject activeObject;
        List<ActiveObjectDetail> activeObjectDetail;

        try {
            activeObject = activeObjectRepository.findByNamespaceAndPackageIdAndDbObjectName(_fbo.namespace, _fbo.package_id, _fbo.db_object_name);
            activeObjectDetail = activeObjectDetailRepository.findAllByHaoid(activeObject.getId());

        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            return null;
        }
        return activeObjectDetail;

    }

    private ActiveObject getActiveObject() {
        ActiveObject activeObject;

        try {
            activeObject = activeObjectRepository.findByNamespaceAndPackageIdAndDbObjectName(_fbo.namespace, _fbo.package_id, _fbo.db_object_name);
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            return null;
        }
        return activeObject;

    }




}
