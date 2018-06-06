package com.hortonworks.faas.nfaas.flow_builder.task.helper;

import com.hortonworks.faas.nfaas.dto.ActiveObject;
import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;
import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;
import com.hortonworks.faas.nfaas.flow_builder.task.HiveDdlGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.List;


@Configuration
public class HiveExternalTableDdl {

    private static final Logger logger = LoggerFactory.getLogger(HiveExternalTableDdl.class);

    private final String delimter = " , ";

    public String generateExternalTableDdl(FlowBuilderOptions fbo,
                                           ActiveObject activeObject,
                                           List<ActiveObjectDetail> aod){

        String sql_head = "CREATE EXTERNAL TABLE %s ( ";
        String sql_body = getSqlBody(aod);
        String sql_tail = " ) STORED AS ORC LOCATION ".concat(
                          " 'hdfs://AWHDP-PRDHA/tmp/aw_hive_stg/stg_%s'");

        sql_head = String.format(sql_head, fbo.db_object_name.toLowerCase());
        sql_tail = String.format(sql_tail, fbo.db_object_name.toLowerCase());

        String sql = sql_head.concat(sql_body).concat(sql_tail);
        logger.debug("generated sql " + sql);
        return sql;
    }

    private String getSqlBody(List<ActiveObjectDetail> aod) {
        StringBuilder sql_body = new StringBuilder();

        int index =1;
        for(ActiveObjectDetail aodetail : aod){
            sql_body.append(aodetail.getColumnName().toLowerCase()).append(" ").append(aodetail.getInferDataType().toLowerCase());
            if(index < aod.size()){
                index ++;
                sql_body.append(delimter);
            }
        }

        return sql_body.toString();
    }
}
