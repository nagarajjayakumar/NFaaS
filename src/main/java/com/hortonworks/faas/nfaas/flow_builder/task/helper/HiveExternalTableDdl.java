package com.hortonworks.faas.nfaas.flow_builder.task.helper;

import com.hortonworks.faas.nfaas.dto.ActiveObject;
import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;
import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;

import java.util.List;

public class HiveExternalTableDdl {

    public String generateExternalTableDdl(FlowBuilderOptions fbo,
                                           ActiveObject activeObject,
                                           List<ActiveObjectDetail> aod){

        String sql_head = "CREATE EXTERNAL TABLE %s ( ";
        String sql_body = getSqlBody(aod);
        String sql_tail = "";

        String.format(sql_head, fbo.db_object_name);

        String sql = sql_head.concat(sql_body).concat(sql_tail);
        return sql;
    }

    private String getSqlBody(List<ActiveObjectDetail> aod) {
        String sql_body = "";

        

        return sql_body;
    }
}
