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
}
