package com.hortonworks.faas.nfaas.xml.parser;


import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlowInfo {

    private  String rootGroupId;

    private  List<PortDTO> ports;

    private  List<ProcessGroupDTO> processGroups;

    /*public FlowInfo() {
        this.rootGroupId = rootGroupId;
        this.ports = (ports == null ? Collections.unmodifiableList(Collections.EMPTY_LIST) :
                Collections.unmodifiableList(new ArrayList<>(ports)));
    }*/

    public void setRootGroupId(String rootGroupId) {
        this.rootGroupId = rootGroupId;
    }



    public void setPorts(List<PortDTO> ports) {
        this.ports = (ports == null ? Collections.unmodifiableList(Collections.EMPTY_LIST) :
                Collections.unmodifiableList(new ArrayList<>(ports)));
    }

    public List<ProcessGroupDTO> getProcessGroups() {
        return processGroups;
    }

    public void setProcessGroups(List<ProcessGroupDTO> processGroups) {
        this.processGroups = (processGroups == null ? Collections.unmodifiableList(Collections.EMPTY_LIST) :
                Collections.unmodifiableList(new ArrayList<>(processGroups)));
    }

    public String getRootGroupId() {
        return rootGroupId;
    }

    public List<PortDTO> getPorts() {
        return ports;
    }

}