package com.hortonworks.faas.nfaas.flow_builder.task.helper;

import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;


@Configuration
public class HiveTxnTableMinorCompactionDml {

    private static final Logger logger = LoggerFactory.getLogger(HiveTxnTableMinorCompactionDml.class);

    private static final String statement_delim = " ; ";


    /**
     *
     * alter table awinternal.maintenancenotificationactts compact 'minor';
     * @param fbo
     * @return
     */
    public String generateTxnTableMinorCompactionDml(FlowBuilderOptions fbo) {

        String sql_head = " alter table   ie_awinternal.%s  compact 'minor' ";
        String sql_tail = statement_delim;

        sql_head = String.format(sql_head, fbo.db_object_name.toLowerCase());

        String sql = sql_head.concat(sql_tail);
        logger.debug("generated minor compaction DML  " + sql);
        return sql;
    }


}