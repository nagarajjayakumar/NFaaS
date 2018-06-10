package com.hortonworks.faas.nfaas.flow_builder.task.helper;

import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;

import java.util.ArrayList;
import java.util.List;

public class HelperUtil {

    private static HelperUtil ourInstance = new HelperUtil();

    public static HelperUtil getInstance() {
        return ourInstance;
    }

    private HelperUtil() {
    }

    private static final String delimter = " , ";
    private static final String and = " and ";
    private static final String equal = " = ";


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
            sql_body.append(aodetail.getColumnName().toLowerCase()).append(" ").append(aodetail.getInferDataType().toLowerCase()).append(", ");
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

    /**
     * This is the method to get the merge clause for the transaction table
     * @param aod
     * @return
     */
    public static String getSqlMergeClause(List<ActiveObjectDetail> aod, String dbo_name) {

        StringBuilder sql_merge_clause = new StringBuilder();
        List<ActiveObjectDetail> keys = getOnlyKeyActiveObjectDetail(aod);
        int index = 1;
        for (ActiveObjectDetail aodetail : keys) {

            /*
            nvl(maintenancenotificationactts.maintenancenotification,"") =
            nvl(maintenancenotificationactts_delta.maintenancenotification,"") and
             */
            if(aodetail.getKey()) {
                sql_merge_clause.append(getLeftOperand(aodetail, dbo_name)).append(" = ").append(getRightOperand(aodetail, dbo_name));
                if (index < keys.size()) {
                    index++;
                    sql_merge_clause.append(and);
                }
            }
        }

        return sql_merge_clause.toString();

    }

    private static List<ActiveObjectDetail> getOnlyKeyActiveObjectDetail(List<ActiveObjectDetail> aod) {
        List<ActiveObjectDetail> result = new ArrayList<>();

        for (ActiveObjectDetail aodetail : aod) {
            if(aodetail.getKey()) {
                result.add(aodetail);
            }
        }
        return result;
    }

    private static String getLeftOperand(ActiveObjectDetail aodetail, String dbo_name) {
        StringBuilder leftOperand = new StringBuilder();

        leftOperand.append(String.format("nvl(%1$s.%2$s, ", dbo_name, aodetail.getColumnName().toLowerCase()));
        String inferDataType  = aodetail.getInferDataType();

        if("int".equalsIgnoreCase(inferDataType.toLowerCase()) ||
                    "bigint".equalsIgnoreCase(inferDataType.toLowerCase())){
            leftOperand.append("0");
        }else{
            leftOperand.append("\"\"");
        }
        leftOperand.append(" ) ");
        return leftOperand.toString();
    }

    private static String getRightOperand(ActiveObjectDetail aodetail, String dbo_name) {
        StringBuilder rightOperand = new StringBuilder();


        rightOperand.append(String.format("nvl(%1$s_delta.%2$s, ", dbo_name, aodetail.getColumnName().toLowerCase()));
        String inferDataType  = aodetail.getInferDataType();

        if("int".equalsIgnoreCase(inferDataType.toLowerCase()) ||
                "bigint".equalsIgnoreCase(inferDataType.toLowerCase())){
            rightOperand.append("0");
        }else{
            rightOperand.append("\"\"");
        }
        rightOperand.append(" ) ");

        return rightOperand.toString();
    }

    /**
     * Method is used to get the merge match clause .. usaully update statement
     * @param aod
     * @param dbo_name
     * @return
     */
    public static String getMergeWhenMatchedClause(List<ActiveObjectDetail> aod, String dbo_name, String clustered_by) {

        StringBuilder mergeMatchedClause = new StringBuilder(" update set ");


        for (ActiveObjectDetail aodetail : aod) {

            // Bucket or clustered by column should not be there in the update statement .. hive restriction
            // Need to revisit after HDP 3.0 release
            // HDP 3.0 Bucket restriction is relaxed

            if(! clustered_by.equalsIgnoreCase(aodetail.getColumnName().toLowerCase())) {
                mergeMatchedClause.append(aodetail.getColumnName().toLowerCase()).append(equal).
                        append(dbo_name).append("_delta.").
                        append(aodetail.getColumnName().toLowerCase()).append(delimter).append(" ");
            }
        }

        mergeMatchedClause.append(" checksum = %1$s_delta.checksum ,").append(" watermark=CURRENT_TIMESTAMP ");

        return mergeMatchedClause.toString();
    }

    public static String getMergeWhenNotMatchedClause(List<ActiveObjectDetail> aod, String dbo_name) {

        StringBuilder mergeNotMatchedClause = new StringBuilder(" insert values ( ");


        for (ActiveObjectDetail aodetail : aod) {
            mergeNotMatchedClause.
                    append(dbo_name).append("_delta.").
                    append(aodetail.getColumnName().toLowerCase()).append(delimter).append(" ");
        }

        mergeNotMatchedClause.append("  %1$s_delta.checksum ,").append("  CURRENT_TIMESTAMP ) ");

        return mergeNotMatchedClause.toString();
    }
}
