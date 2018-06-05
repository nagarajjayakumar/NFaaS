# nFaaS
NiFi Flow as a Service
========================

Automation tool for delivering NiFi flow as a Service

Built based on micro services + Angular JS architecture    

Embedded authentication and Oauth2 authorization server 

Integrated with NiFi Secured cluster – via NiFi bearer token services

In built NiFi template validator


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

http://localhost:8084/mdb/getaod?namespace=DataLake.Deltaviews.TransactionViews&package_id=DataLake.Deltaviews.TransactionViews&db_object_name=InstallationOwnershipTS



SIGN: Naga Jay @nagajay_

CICD: Yogeshprabhu @yogesh_