package com.hortonworks.faas.nfaas.flow_builder.task;

import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;

import java.util.ArrayList;
import java.util.List;

public class HiveDdlGenerator implements  Task{

    public String task = "hive_ddl_generator";

    @Override
    public void doWork() {

        List<ActiveObjectDetail> aod = getActiveObjectDetail();
    }

    private List<ActiveObjectDetail> getActiveObjectDetail() {
        List<ActiveObjectDetail> aod = new ArrayList<>();


        return aod;
    }
}
