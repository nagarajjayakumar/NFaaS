package com.hortonworks.faas.nfaas.flow_builder.task.helper;

import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;

import java.util.List;

public class HelperUtil {

    private static HelperUtil ourInstance = new HelperUtil();

    public static HelperUtil getInstance() {
        return ourInstance;
    }

    private HelperUtil() {
    }

    private static final String delimter = " , ";

    public static String getSqlBody(List<ActiveObjectDetail> aod) {
        StringBuilder sql_body = new StringBuilder();

        int index = 1;
        for (ActiveObjectDetail aodetail : aod) {
            sql_body.append(aodetail.getColumnName().toLowerCase()).append(" ").append(aodetail.getInferDataType().toLowerCase());
            if (index < aod.size()) {
                index++;
                sql_body.append(delimter);
            }
        }

        return sql_body.toString();
    }

    public static String getSqlBodyWithCheckSumAndWaterMark(List<ActiveObjectDetail> aod) {
        StringBuilder sql_body = new StringBuilder();

        for (ActiveObjectDetail aodetail : aod) {
            sql_body.append(aodetail.getColumnName().toLowerCase()).append(" ").append(aodetail.getInferDataType().toLowerCase());
        }
        sql_body.append(" checksum string, ").append(" watermark timestamp ");
        return sql_body.toString();
    }

    public static String getStagingSelectSqlBody(List<ActiveObjectDetail> aod, String dboName) {
        StringBuilder sql_body = new StringBuilder();

        for (ActiveObjectDetail aodetail : aod) {
            sql_body.append("stg_").append(dboName).append(".").append(aodetail.getColumnName().toLowerCase()).append(delimter).append(" ");
        }

        String md5HashClause = getMd5HashClause(aod);

        sql_body.append(" ").append(md5HashClause).append(" ");
        sql_body.append(" CURRENT_TIMESTAMP ");
        return sql_body.toString();
    }

    private static String getMd5HashClause(List<ActiveObjectDetail> aod) {
        StringBuilder md5HashClause = new StringBuilder();
        md5HashClause.append("md5(CONCAT(");

        int index = 1;
        for (ActiveObjectDetail aodetail : aod) {
            md5HashClause.append(" (nvl(").append(aodetail.getColumnName().toLowerCase()).append(" ,\"\")) ");
            if (index < aod.size()) {
                index++;
                md5HashClause.append(delimter);
            }
        }

        md5HashClause.append(")),");
        return md5HashClause.toString();
    }
}
