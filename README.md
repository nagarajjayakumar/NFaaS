# nfaas
Nifi Flow as a Service
========================

Automation tool for delivering Nifi flow as a Servive

Built based on micro services + Angular JS architecture    

Embedded authentication and Oauth2 authorization server 

Integrated with NIFI Secured cluster – via NIFI bearer token services

In built NIFI template validator


HDFM – Config
==============

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

Runs on embedded tomcat server – Generated artificat war file
