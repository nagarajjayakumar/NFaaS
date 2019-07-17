package com.hortonworks.faas.nfaas.dto;

import javax.persistence.*;
import java.util.Date;


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
    java.util.Date createdAt;
    java.util.Date updatedAt;

//    @ManyToOne
//    @JoinColumn(name = "haoid")
//    ActiveObject activeObject;

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

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public Date getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Date updatedAt) {
        this.updatedAt = updatedAt;
    }

    //
//    public ActiveObject getActiveObject() {
//        return activeObject;
//    }
//
//    public void setActiveObject(ActiveObject activeObject) {
//        this.activeObject = activeObject;
//    }
}
