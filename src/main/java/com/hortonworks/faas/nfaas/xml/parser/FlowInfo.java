package com.hortonworks.faas.nfaas.xml.parser;


import org.apache.nifi.web.api.dto.PortDTO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlowInfo {

    private final String rootGroupId;

    private final List<PortDTO> ports;

    public FlowInfo(final String rootGroupId, final List<PortDTO> ports) {
        this.rootGroupId = rootGroupId;
        this.ports = (ports == null ? Collections.unmodifiableList(Collections.EMPTY_LIST) :
                Collections.unmodifiableList(new ArrayList<>(ports)));
    }

    public String getRootGroupId() {
        return rootGroupId;
    }

    public List<PortDTO> getPorts() {
        return ports;
    }

}