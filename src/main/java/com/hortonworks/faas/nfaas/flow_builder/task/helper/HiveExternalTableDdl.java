package com.hortonworks.faas.nfaas.flow_builder.task.helper;

import com.hortonworks.faas.nfaas.dto.ActiveObject;
import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;
import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.List;


@Configuration
public class HiveExternalTableDdl {

    private static final Logger logger = LoggerFactory.getLogger(HiveExternalTableDdl.class);

    public String generateExternalTableDdl(FlowBuilderOptions fbo,
                                           List<ActiveObjectDetail> aod) {

        String sql_head = "CREATE EXTERNAL TABLE ie_awinternal.stg_%s ( ";
        String sql_body = HelperUtil.getSqlBody(aod);
        String sql_tail = " ) STORED AS ORC LOCATION ".concat(
                " 'hdfs://AWHDP-QAHA/tmp/aw_hive_stg/stg_%s' ");

        sql_head = String.format(sql_head, fbo.db_object_name.toLowerCase());
        sql_tail = String.format(sql_tail, fbo.db_object_name.toLowerCase());

        String sql = sql_head.concat(sql_body).concat(sql_tail);
        logger.debug("generated sql " + sql);
        return sql;
    }


}
