package com.hortonworks.faas.nfaas.dto;

public class FlowProcessor {

    private Long id;   // Vertex ID
    private String procName; // 3 Processor Name
    private String procId; // 4 processor ID
    private FlowProcessGroup flowProcessGroup;

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

    public FlowProcessGroup getFlowProcessGroup() {
        return flowProcessGroup;
    }

    public void setFlowProcessGroup(FlowProcessGroup flowProcessGroup) {
        this.flowProcessGroup = flowProcessGroup;
    }

    @Override
    public String toString() {
        return "FlowProcessor{" +
                "id=" + id +
                ", procName='" + procName + '\'' +
                ", procId='" + procId + '\'' +
                ", flowProcessGroup=" + flowProcessGroup +
                '}';
    }
}
