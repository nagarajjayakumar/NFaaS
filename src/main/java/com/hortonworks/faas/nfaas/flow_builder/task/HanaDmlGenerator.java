package com.hortonworks.faas.nfaas.flow_builder.task;

import com.hortonworks.faas.nfaas.dto.ActiveObject;
import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;
import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;
import com.hortonworks.faas.nfaas.flow_builder.task.helper.HanaViewSelectDml;
import com.hortonworks.faas.nfaas.orm.ActiveObjectDetailRepository;
import com.hortonworks.faas.nfaas.orm.ActiveObjectRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.util.List;


@Configuration
public class HanaDmlGenerator implements Task {

    private static final Logger logger = LoggerFactory.getLogger(HanaDmlGenerator.class);

    public String task = "hana_dml_generator";

    private FlowBuilderOptions _fbo = null;

    @Autowired
    private ActiveObjectRepository activeObjectRepository;

    @Autowired
    private ActiveObjectDetailRepository activeObjectDetailRepository;

    @Autowired
    HanaViewSelectDml hanaViewSelectDml;

    public void doWork(FlowBuilderOptions fbo){
        logger.info("Inside the task " +task);
    }


    public String getMaxValueColumns(FlowBuilderOptions fbo)
    {
        logger.info(String.format("started %s !! ", task));
        this._fbo = fbo;
        List<ActiveObjectDetail> aod = this.getActiveObjectDetail();
        String maxValueColumns = hanaViewSelectDml.getMaxValueColumns(fbo, aod);
        logger.info(String.format("ended %s !! ", task));
        return maxValueColumns;

    }


    public String getOrderByClause(FlowBuilderOptions fbo)
    {
        logger.info(String.format("started %s !! ", task));
        this._fbo = fbo;
        List<ActiveObjectDetail> aod = this.getActiveObjectDetail();
        String orderByClause = hanaViewSelectDml.getOrderByClause(fbo, aod);
        logger.info(String.format("ended %s !! ", task));
        return orderByClause;

    }


    private List<ActiveObjectDetail> getActiveObjectDetail() {
        ActiveObject activeObject;
        java.util.List<com.hortonworks.faas.nfaas.dto.ActiveObjectDetail> activeObjectDetail;

        try {
            activeObject = this.getActiveObject();
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
