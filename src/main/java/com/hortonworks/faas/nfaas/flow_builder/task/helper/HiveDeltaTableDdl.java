package com.hortonworks.faas.nfaas.flow_builder.task.helper;

import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;
import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.List;


@Configuration
public class HiveDeltaTableDdl {

    private static final Logger logger = LoggerFactory.getLogger(HiveDeltaTableDdl.class);


    public String generateDeltaTableDdl(FlowBuilderOptions fbo,
                                           List<ActiveObjectDetail> aod) {

        String sql_head = "CREATE TABLE awinternal.%s_delta ( ";
        String sql_body = HelperUtil.getSqlBody(aod);
        String sql_tail = " ) CLUSTERED BY ( " +
                "   %1$s )   INTO %2$s BUCKETS " +
                " STORED AS ORC  " +
                " TBLPROPERTIES (   " +
                "   'compactor.mapreduce.map.memory.mb'='2048',  " +
                "   'compactorthreshold.hive.compactor.delta.num.threshold'='1',  " +
                "   'compactorthreshold.hive.compactor.delta.pct.threshold'='0.5', " +
                "   'transactional'='true');                ";

        sql_head = String.format(sql_head, fbo.db_object_name.toLowerCase());
        sql_tail = String.format(sql_tail, fbo.clustered_by, fbo.buckets);

        String sql = sql_head.concat(sql_body).concat(sql_tail);
        logger.debug("generated sql " + sql);
        return sql;
    }


}