package com.hortonworks.faas.nfaas.flow_builder.task.helper;

import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;
import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.List;


@Configuration
public class HiveTxnTableMergeDml {

    private static final Logger logger = LoggerFactory.getLogger(HiveTxnTableMergeDml.class);

    private static final String statement_delim = " ; ";


    public String generateTxnTableMergeDml(FlowBuilderOptions fbo,
                                           List<ActiveObjectDetail> aod) {

        String sql_head = "merge into ie_awinternal.%1$s  as %1$s  using ie_awinternal.%1$s_delta as %1$s_delta on ";
        String merge_clause =" ( ".concat(HelperUtil.getSqlMergeClause(aod, fbo.db_object_name.toLowerCase() )).concat(" ) ");

        String when_matched_clause_head = " when matched and %1$s.checksum <> %1$s_delta.checksum then ";
        String when_matched_clause_body = HelperUtil.getMergeWhenMatchedClause(aod, fbo.db_object_name.toLowerCase(), fbo.clustered_by.toLowerCase() );

        String when_not_matched_clause_head = " when not matched then ";
        String when_not_matched_clause_body = HelperUtil.getMergeWhenNotMatchedClause(aod, fbo.db_object_name.toLowerCase() );

        String sql_tail = statement_delim;

        sql_head = String.format(sql_head, fbo.db_object_name.toLowerCase());
        when_matched_clause_head  =  String.format(when_matched_clause_head, fbo.db_object_name.toLowerCase());
        when_matched_clause_body = String.format(when_matched_clause_body, fbo.db_object_name.toLowerCase());
        when_not_matched_clause_body = String.format(when_not_matched_clause_body, fbo.db_object_name.toLowerCase());

        String sql = sql_head.concat(merge_clause).
                              concat(when_matched_clause_head).
                              concat(when_matched_clause_body).
                              concat(when_not_matched_clause_head).
                              concat(when_not_matched_clause_body).concat(sql_tail);

        logger.debug("generated Merge DML  " + sql);
        return sql;
    }


}