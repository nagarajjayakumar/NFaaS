package com.hortonworks.faas.nfaas.dto;

import org.joda.time.DateTime;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.util.Set;


@Entity
@Table(name = "hanadb_active_object")
public class ActiveObject {

    Long id;
    String namespace;
    String packageId;
    String dbObjectName;
    String dbObjectType;
    String dbObjectTypeSuffix;
    Integer version;
    DateTime activatedAt;
    String activatedBy;
    Boolean isActive;
    Set<ActiveObjectDetail> activeObjectDetail;
    DateTime createdAt;
    DateTime updatedAt;

    public ActiveObject() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getPackageId() {
        return packageId;
    }

    public void setPackageId(String packageId) {
        this.packageId = packageId;
    }

    public String getDbObjectName() {
        return dbObjectName;
    }

    public void setDbObjectName(String dbObjectName) {
        this.dbObjectName = dbObjectName;
    }

    public String getDbObjectType() {
        return dbObjectType;
    }

    public void setDbObjectType(String dbObjectType) {
        this.dbObjectType = dbObjectType;
    }

    public String getDbObjectTypeSuffix() {
        return dbObjectTypeSuffix;
    }

    public void setDbObjectTypeSuffix(String dbObjectTypeSuffix) {
        this.dbObjectTypeSuffix = dbObjectTypeSuffix;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public DateTime getActivatedAt() {
        return activatedAt;
    }

    public void setActivatedAt(DateTime activatedAt) {
        this.activatedAt = activatedAt;
    }

    public String getActivatedBy() {
        return activatedBy;
    }

    public void setActivatedBy(String activatedBy) {
        this.activatedBy = activatedBy;
    }

    public Boolean getActive() {
        return isActive;
    }

    public void setActive(Boolean active) {
        isActive = active;
    }

    @OneToMany(mappedBy = "activeObject", cascade = CascadeType.ALL)
    public Set<ActiveObjectDetail> getActiveObjectDetail() {
        return activeObjectDetail;
    }

    public void setActiveObjectDetail(Set<ActiveObjectDetail> activeObjectDetail) {
        this.activeObjectDetail = activeObjectDetail;
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
}
