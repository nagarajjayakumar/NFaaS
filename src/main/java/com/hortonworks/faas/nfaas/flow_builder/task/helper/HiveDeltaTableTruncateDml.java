package com.hortonworks.faas.nfaas.flow_builder.task.helper;

import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;
import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.List;


@Configuration
public class HiveDeltaTableTruncateDml {

    private static final String statement_delim = " ; ";

    private static final Logger logger = LoggerFactory.getLogger(HiveDeltaTableTruncateDml.class);


    // truncate table awinternal.maintenancenotificationactts_delta;
    public String generateDeltaTableTruncateDml(FlowBuilderOptions fbo) {

        String sql_head = " truncate  table  ie_awinternal.%s_delta  ";
        String sql_tail = statement_delim;

        sql_head = String.format(sql_head, fbo.db_object_name.toLowerCase());

        String sql = sql_head.concat(sql_tail);
        logger.debug("generated truncate DML  " + sql);
        return sql;
    }


}