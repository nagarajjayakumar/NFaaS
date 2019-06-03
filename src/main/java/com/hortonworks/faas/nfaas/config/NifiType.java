package com.hortonworks.faas.nfaas.config;

public enum NifiType {

    PROCESS_GROUP("processGroup");

    public String type;

    private NifiType(String type) {
        this.type = type;
    }

    public String getState() {
        return type;
    }


}