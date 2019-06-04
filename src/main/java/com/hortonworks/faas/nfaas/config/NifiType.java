package com.hortonworks.faas.nfaas.config;

public enum NifiType {

    PROCESS_GROUP("processGroup"),
    PROCESSOR("processor");

    public String type;

    NifiType(String type) {
        this.type = type;
    }

    public String getState() {
        return type;
    }


}