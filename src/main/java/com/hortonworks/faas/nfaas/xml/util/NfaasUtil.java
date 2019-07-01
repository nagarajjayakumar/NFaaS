package com.hortonworks.faas.nfaas.xml.util;

import com.hortonworks.faas.nfaas.dto.FlowProcessor;

public class NfaasUtil {

    public static boolean isEmptyFlowProcessor(FlowProcessor fp) {
        boolean isEmptyFlowProcessor = false;

        if(fp == null || fp.getId() == null || fp.getId() <=0 )
             isEmptyFlowProcessor = true;

        return isEmptyFlowProcessor;
    }
}
