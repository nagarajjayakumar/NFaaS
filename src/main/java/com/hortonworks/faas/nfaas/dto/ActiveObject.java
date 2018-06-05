package com.hortonworks.faas.nfaas.dto;

import org.joda.time.DateTime;

import javax.persistence.*;
import java.util.Date;
import java.util.Set;


@Entity
@Table(name = "hanadb_active_object")
public class ActiveObject {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    Long id;
    String namespace;
    String packageId;
    String dbObjectName;
    String dbObjectType;
    String dbObjectTypeSuffix;
    Integer version;
    java.util.Date activatedAt;
    String activatedBy;
    Boolean isActive;
    java.util.Date createdAt;
    java.util.Date updatedAt;

//    @OneToMany(mappedBy = "activeObject", cascade = CascadeType.ALL)
//    Set<ActiveObjectDetail> activeobjectdetails;

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


//    public Set<ActiveObjectDetail> getActiveobjectdetails() {
//        return activeobjectdetails;
//    }
//
//    public void setActiveobjectdetails(Set<ActiveObjectDetail> activeobjectdetails) {
//        this.activeobjectdetails = activeobjectdetails;
//    }


    public Date getActivatedAt() {
        return activatedAt;
    }

    public void setActivatedAt(Date activatedAt) {
        this.activatedAt = activatedAt;
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
}
