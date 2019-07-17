package com.hortonworks.faas.nfaas.xml.parser;


import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;

import java.util.List;

public class FlowInfo {

    private String rootGroupId;

    private String rootGroupName;

    private List<PortDTO> ports;

    private List<ProcessGroupDTO> processGroups;

    private List<ProcessorDTO> processors;

    private List<ConnectionDTO> connections;

    private FlowEncodingVersion flowEncodingVersion;

    /*public FlowInfo() {
        this.rootGroupId = rootGroupId;
        this.ports = (ports == null ? Collections.unmodifiableList(Collections.EMPTY_LIST) :
                Collections.unmodifiableList(new ArrayList<>(ports)));
    }*/

    public List<ProcessGroupDTO> getProcessGroups() {
        return processGroups;
    }

    public void setProcessGroups(List<ProcessGroupDTO> processGroups) {
//        this.processGroups = (processGroups == null ? Collections.unmodifiableList(Collections.EMPTY_LIST) :
//                Collections.unmodifiableList(new ArrayList<>(processGroups)));
        this.processGroups = processGroups;
    }

    public String getRootGroupId() {
        return rootGroupId;
    }

    public void setRootGroupId(String rootGroupId) {
        this.rootGroupId = rootGroupId;
    }

    public List<PortDTO> getPorts() {
        return ports;
    }

    public void setPorts(List<PortDTO> ports) {
//        this.ports = (ports == null ? Collections.unmodifiableList(Collections.EMPTY_LIST) :
//                Collections.unmodifiableList(new ArrayList<>(ports)));
        this.ports = ports;
    }

    public String getRootGroupName() {
        return rootGroupName;
    }

    public void setRootGroupName(String rootGroupName) {
        this.rootGroupName = rootGroupName;
    }

    public List<ProcessorDTO> getProcessors() {
        return processors;
    }

    public void setProcessors(List<ProcessorDTO> processors) {
        this.processors = processors;
    }

    public FlowEncodingVersion getFlowEncodingVersion() {
        return flowEncodingVersion;
    }

    public void setFlowEncodingVersion(FlowEncodingVersion flowEncodingVersion) {
        this.flowEncodingVersion = flowEncodingVersion;
    }

    public List<ConnectionDTO> getConnections() {
        return connections;
    }

    public void setConnections(List<ConnectionDTO> connections) {
        this.connections = connections;
    }
}