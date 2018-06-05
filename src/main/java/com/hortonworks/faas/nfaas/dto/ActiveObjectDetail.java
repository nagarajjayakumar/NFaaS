package com.hortonworks.faas.nfaas.dto;

import org.joda.time.DateTime;

import javax.persistence.*;


@Entity
@Table(name = "hanadb_active_object_detail")
public class ActiveObjectDetail {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    Long id;
    Long haoid;
    String columnName;
    Boolean isKey;
    Integer col_order;
    Boolean attributeHierarchyActive;
    Boolean displayAttribute;
    String defaultDescription;
    String sourceObjectName;
    String sourceColumnName;
    String sourceDataType;
    String inferDataType;
    Boolean isRequiredForFlow;
    DateTime createdAt;
    DateTime updatedAt;
    ActiveObject activeObject;

    public ActiveObjectDetail() {
    }


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getHaoid() {
        return haoid;
    }

    public void setHaoid(Long haoid) {
        this.haoid = haoid;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public Boolean getKey() {
        return isKey;
    }

    public void setKey(Boolean key) {
        isKey = key;
    }

    public Integer getCol_order() {
        return col_order;
    }

    public void setCol_order(Integer col_order) {
        this.col_order = col_order;
    }

    public Boolean getAttributeHierarchyActive() {
        return attributeHierarchyActive;
    }

    public void setAttributeHierarchyActive(Boolean attributeHierarchyActive) {
        this.attributeHierarchyActive = attributeHierarchyActive;
    }

    public Boolean getDisplayAttribute() {
        return displayAttribute;
    }

    public void setDisplayAttribute(Boolean displayAttribute) {
        this.displayAttribute = displayAttribute;
    }

    public String getDefaultDescription() {
        return defaultDescription;
    }

    public void setDefaultDescription(String defaultDescription) {
        this.defaultDescription = defaultDescription;
    }

    public String getSourceObjectName() {
        return sourceObjectName;
    }

    public void setSourceObjectName(String sourceObjectName) {
        this.sourceObjectName = sourceObjectName;
    }

    public String getSourceColumnName() {
        return sourceColumnName;
    }

    public void setSourceColumnName(String sourceColumnName) {
        this.sourceColumnName = sourceColumnName;
    }

    public String getSourceDataType() {
        return sourceDataType;
    }

    public void setSourceDataType(String sourceDataType) {
        this.sourceDataType = sourceDataType;
    }

    public String getInferDataType() {
        return inferDataType;
    }

    public void setInferDataType(String inferDataType) {
        this.inferDataType = inferDataType;
    }

    public Boolean getRequiredForFlow() {
        return isRequiredForFlow;
    }

    public void setRequiredForFlow(Boolean requiredForFlow) {
        isRequiredForFlow = requiredForFlow;
    }

    public DateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(DateTime createdAt) {
        this.createdAt = createdAt;
    }

    public DateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(DateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    @ManyToOne
    @JoinColumn(name = "haoid")
    public ActiveObject getActiveObject() {
        return activeObject;
    }

    public void setActiveObject(ActiveObject activeObject) {
        this.activeObject = activeObject;
    }
}
