package com.hortonworks.faas.nfaas.flow_builder.task.helper;

import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;
import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.List;


@Configuration
public class HiveStagingTableSelectDml {

    private static final Logger logger = LoggerFactory.getLogger(HiveStagingTableSelectDml.class);



    public String generateStagingTableSelectDml(FlowBuilderOptions fbo,
                                                List<ActiveObjectDetail> aod) {

        String sql_head = " select distinct  ";
        String sql_body = HelperUtil.getStagingSelectSqlBody(aod, fbo.db_object_name.toLowerCase());
        String sql_tail = " from ie_awinternal.%s ";

        sql_head = String.format(sql_head);
        sql_tail = String.format(sql_tail, fbo.db_object_name.toLowerCase());

        String sql = sql_head.concat(sql_body).concat(sql_tail);
        logger.debug("generated staging table select DML  " + sql);
        return sql;
    }


}