package com.hortonworks.faas.nfaas.dto;

import java.util.List;

public class FlowProcessor {

    private Long id;   // Vertex ID
    private String procName; // 3 Processor Name
    private String procId; // 4 processor ID
    private List<FlowProcessGroup> parentProcessGroups;
    private List<FlowProcessGroup> upstreamDependentProcessGroups;
    private List<FlowProcessGroup> downstreamDependentProcessGroups;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getProcName() {
        return procName;
    }

    public void setProcName(String procName) {
        this.procName = procName;
    }

    public String getProcId() {
        return procId;
    }

    public void setProcId(String procId) {
        this.procId = procId;
    }

    public List<FlowProcessGroup> getParentProcessGroups() {
        return parentProcessGroups;
    }

    public void setParentProcessGroups(List<FlowProcessGroup> parentProcessGroups) {
        this.parentProcessGroups = parentProcessGroups;
    }

    public List<FlowProcessGroup> getUpstreamDependentProcessGroups() {
        return upstreamDependentProcessGroups;
    }

    public void setUpstreamDependentProcessGroups(List<FlowProcessGroup> upstreamDependentProcessGroups) {
        this.upstreamDependentProcessGroups = upstreamDependentProcessGroups;
    }

    public List<FlowProcessGroup> getDownstreamDependentProcessGroups() {
        return downstreamDependentProcessGroups;
    }

    public void setDownstreamDependentProcessGroups(List<FlowProcessGroup> downstreamDependentProcessGroups) {
        this.downstreamDependentProcessGroups = downstreamDependentProcessGroups;
    }

    @Override
    public String toString() {
        return "FlowProcessor{" +
                "id=" + id +
                ", procName='" + procName + '\'' +
                ", procId='" + procId + '\'' +
                ", parentProcessGroups=" + parentProcessGroups +
                ", upstreamDependentProcessGroups=" + upstreamDependentProcessGroups +
                ", downstreamDependentProcessGroups=" + downstreamDependentProcessGroups +
                '}';
    }
}
