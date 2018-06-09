package com.hortonworks.faas.nfaas.flow_builder.task.helper;

import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;
import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.util.List;


@Configuration
public class HiveDeltaTableInsertDml {

    @Autowired
    private HiveStagingTableSelectDml hiveStagingTableSelectDml;

    private static final Logger logger = LoggerFactory.getLogger(HiveDeltaTableInsertDml.class);


    public String generateDeltaTableInsertDml(FlowBuilderOptions fbo,
                                              List<ActiveObjectDetail> aod) {

        String sql_head = "insert into table  ie_awinternal.%s_delta ( ";
        String sql_body = hiveStagingTableSelectDml.generateStagingTableSelectDml(fbo, aod);
        String sql_tail = " ";

        sql_head = String.format(sql_head, fbo.db_object_name.toLowerCase());
        sql_tail = String.format(sql_tail, fbo.clustered_by, fbo.buckets);

        String sql = sql_head.concat(sql_body).concat(sql_tail);
        logger.debug("generated insert DML  " + sql);
        return sql;
    }


}