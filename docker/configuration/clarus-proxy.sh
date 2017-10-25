#!/bin/bash

source /etc/clarus/clarus-proxy.conf
sed -i "s/EMBEDDED_MONGO_DB_IP/$EMBEDDED_MONGO_DB_IP/g" /etc/clarus/clarus-keystore.conf
sed -i "s/EMBEDDED_MONGO_DB_PORT/$EMBEDDED_MONGO_DB_PORT/g" /etc/clarus/clarus-keystore.conf

CLARUS_PROXY_VERSION=1.0.1

$JAVA_COMMAND -Djava.ext.dirs=$CLARUS_LIBRARIES_PATH:$JAVA_HOME/lib/ext -jar $CLARUS_BASE_PATH/proxy-main-"$CLARUS_PROXY_VERSION".jar -sp "$CLARUS_SECURITY_POLICY" "$CLARUS_CLOUD_IP":"$CLARUS_CLOUD_PORT"

