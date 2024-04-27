import pysnooper

@pysnooper.snoop()
def sum(x,y):
    print(x+y)

sum(1,2)


// Initialisation
// This code block only needs to be run once to create the init script for the cluster (file remains on restart)

// Get the cluster name
var clusterName = dbutils.widgets.get("cluster")

// Create dbfs:/databricks/init/ if it doesn’t exist.
dbutils.fs.mkdirs("dbfs:/databricks/init/")

// Create a directory named (clusterName) using Databricks File System - DBFS.
dbutils.fs.mkdirs(s"dbfs:/databricks/init/$clusterName/")

// Create the adal4j script.
dbutils.fs.put(s"/databricks/init/$clusterName/adal4j-install.sh","""
#!/bin/bash
wget --quiet -O /mnt/driver-daemon/jars/adal4j-1.6.0.jar http://central.maven.org/maven2/com/microsoft/azure/adal4j/1.6.0/adal4j-1.6.0.jar
wget --quiet -O /mnt/jars/driver-daemon/adal4j-1.6.0.jar http://central.maven.org/maven2/com/microsoft/azure/adal4j/1.6.0/adal4j-1.6.0.jar""", true)

// Create the oauth2 script.
dbutils.fs.put(s"/databricks/init/$clusterName/oauth2-install.sh","""
#!/bin/bash
wget --quiet -O /mnt/driver-daemon/jars/oauth2-oidc-sdk-5.24.1.jar http://central.maven.org/maven2/com/nimbusds/oauth2-oidc-sdk/5.24.1/oauth2-oidc-sdk-5.24.1.jar
wget --quiet -O /mnt/jars/driver-daemon/oauth2-oidc-sdk-5.24.1.jar http://central.maven.org/maven2/com/nimbusds/oauth2-oidc-sdk/5.24.1/oauth2-oidc-sdk-5.24.1.jar""", true)

// Create the json script.
dbutils.fs.put(s"/databricks/init/$clusterName/json-smart-install.sh","""
#!/bin/bash
wget --quiet -O /mnt/driver-daemon/jars/json-smart-1.1.1.jar http://central.maven.org/maven2/net/minidev/json-smart/1.1.1/json-smart-1.1.1.jar
wget --quiet -O /mnt/jars/driver-daemon/json-smart-1.1.1.jar http://central.maven.org/maven2/net/minidev/json-smart/1.1.1/json-smart-1.1.1.jar""", true)

// Create the jwt script.
dbutils.fs.put(s"/databricks/init/$clusterName/jwt-install.sh","""
#!/bin/bash
wget --quiet -O /mnt/driver-daemon/jars/nimbus-jose-jwt-7.0.1.jar http://central.maven.org/maven2/com/nimbusds/nimbus-jose-jwt/7.0.1/nimbus-jose-jwt-7.0.1.jar
wget --quiet -O /mnt/jars/driver-daemon/nimbus-jose-jwt-7.0.1.jar http://central.maven.org/maven2/com/nimbusds/nimbus-jose-jwt/7.0.1/nimbus-jose-jwt-7.0.1.jar""", true)

// Check that the cluster-specific init script exists.
display(dbutils.fs.ls(s"dbfs:/databricks/init/$clusterName/"))
