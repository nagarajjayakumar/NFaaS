package com.hortonworks.faas.nfaas.dto;

public class ProcessGroups {

    private Boolean isRoot;

    private Long id;   // Vertex ID

    private String pgName; // 4 process group name

    private String pgId; // process group id

    public Boolean getRoot() {
        return isRoot;
    }

    public void setRoot(Boolean root) {
        isRoot = root;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getPgName() {
        return pgName;
    }

    public void setPgName(String pgName) {
        this.pgName = pgName;
    }

    public String getPgId() {
        return pgId;
    }

    public void setPgId(String pgId) {
        this.pgId = pgId;
    }
}
