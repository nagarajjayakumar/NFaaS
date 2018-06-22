# nFaaS
NiFi Flow as a Service
========================

Automation tool for delivering NiFi flow as a Service

Built based on micro services + Angular JS architecture    

Embedded authentication and Oauth2 authorization server 

Integrated with NiFi Secured cluster – via NiFi bearer token services

In built NiFi template validator

#

![alt text](https://github.com/nagarajjayakumar/NFaaS/blob/master/nFaaS_Arch.png)

#

![alt text](https://github.com/nagarajjayakumar/NFaaS/blob/master/nFaaS_Architecture.png)


nFaaS – Config
=================

Minimal Configuration

Supports 2 modes of template import/upload between environments
      File based 
      URI – Bit Bucket Integration

Deploy and un-deploy templates or individual process groups 

Enable Remote process group as part of the deployment procedure

Auto queue flush management – Enabled by default

Disabled for more controlled deployment

Un-deploy process group based on PGID – For Development Cluster only

Configuration is externalized 

Runs on embedded tomcat server – Generated artifact war file

Dev tools
============
mdbdetails

```
http://localhost:8084/mdb/getaod?namespace=DataLake.Deltaviews.TransactionViews&package_id=DataLake.Deltaviews.TransactionViews&db_object_name=InstallationOwnershipTS
```
```
http://localhost:8084/faas/createhiveddl?namespace=DataLake.Deltaviews.TransactionViews&package_id=DataLake.Deltaviews.TransactionViews&db_object_name=InstallationOwnershipTS
```
```
http://localhost:8084/faas/createhiveddl?namespace=DataLake.Deltaviews.TransactionViews&package_id=DataLake.Deltaviews.TransactionViews&db_object_name=InstallationOwnershipTS&buckets=32&clustered_by=documentnumber
```
```
http://localhost:8084/faas/createhivetable?namespace=DataLake.Deltaviews.TransactionViews&package_id=DataLake.Deltaviews.TransactionViews&db_object_name=InstallationOwnershipTS&buckets=32&clustered_by=installation
```
```
http://localhost:8084/faas/hanaingestionpipeline?namespace=DataLake.Deltaviews.TransactionViews&package_id=DataLake.Deltaviews.TransactionViews&db_object_name=InstallationOwnershipTS&buckets=32&clustered_by=installation
```

Hana Ingestion Pipe Line Sample Rest URL 
=========================================
```
http://localhost:8084/faas/hanaingestionpipeline?namespace=_SYS_BIC&package_id=DataLake.Deltaviews.TransactionViews&db_object_name=InstallationOwnershipTS&buckets=32&clustered_by=installation
```

SIGN : Naga Jay @nagajay_

Contributor : Adam Michalsky @Michalsky_

CICD : Yogeshprabhu @yogesh_
