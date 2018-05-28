package com.hortonworks.faas.nfaas.config;

public enum EntityState {

    ENABLED("ENABLED"),
    DISABLED("DISABLED"),
    RUNNING("RUNNING"),
    STOPPED("STOPPED"),
    DELETE("DELETE"),
    INVALID("INVALID"),
    TRANSMIT_TRUE("true"),
    TRANSMIT_FALSE("false");

    public String state;

    private EntityState(String state) {
        this.state = state;
    }

    public String getState() {
        return state;
    }


}
