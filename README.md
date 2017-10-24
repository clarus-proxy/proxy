
# Clarus Proxy

[![Build Status](https://travis-ci.org/clarus-proxy/proxy.svg?branch=develop)](https://travis-ci.org/clarus-proxy/proxy)
[![GitHub issues](https://img.shields.io/github/issues/clarus-proxy/proxy.svg)](https://github.com/clarus-proxy/proxy/issues)
[![GitHub forks](https://img.shields.io/github/forks/clarus-proxy/proxy.svg)](https://github.com/clarus-proxy/proxy/network)
[![Docker Build Status](https://img.shields.io/docker/build/clarus/proxy.svg)](https://hub.docker.com/r/clarus/proxy/)
[![Docker badge](https://img.shields.io/docker/pulls/clarus/proxy.svg)](https://hub.docker.com/r/clarus/proxy/)
[![Docker Stars](https://img.shields.io/docker/stars/clarus/proxy.svg)](https://hub.docker.com/r/clarus/proxy/)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/c40f325801064603ba1153fed927dd77)](https://www.codacy.com/app/romain-ferrari/proxy?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=clarus-proxy/proxy&amp;utm_campaign=Badge_Grade)

CLARUS proxy component

## Pre Requisites

* Git
* OpenJDK >= 8.0
* Maven 3
* Gradle
* MongoDB

## Binaries

A debian package is available for Ubuntu based distributions. It has been tested on `Ubuntu 16.04.2 LTS` This packages is available for download at [CLARUS-Proxy latest release ](https://github.com/clarus-proxy/proxy/releases/latest)

In order to install the CLARUS proxy you will have to run:

```bash
curl --output clarus-proxy_1.0.1.deb --location https://github.com/clarus-proxy/proxy/releases/download/v1.0.1/clarus-proxy_1.0.1.deb && sudo apt install -y ./clarus-proxy_1.0.1.deb
````

## Usage

Some sample policies are provided within the project. You can find them in the [sample policy](main/src/test/resources) directory.

Some configuration is needed. You will need to edit the file `/etc/clarus/clarus-proxy.conf`

* `JAVA_HOME` : The directory of your JRE
* `CLARUS_SECURITY_POLICY`: The path of the security policy used by CLARUS to protect the data
* `CLARUS_LIBRARIES_PATH` : Path to CLARUS' libraries
* `CLARUS_CLOUD_IP` : IP of the instance that runs your endpoint (ie postgres server)
* `CLARUS_CLOUD_PORT` : Port of the service running on your endpoint
* `CLARUS_BASE_PATH` : CLARUS' installation directory

The command 
```bash
clarus-proxy
``` 
That will start the CLARUS server. Depending on the port choosen, you may have to run it using `sudo` 

The CLARUS proxy is not daemonized, if you want it to run as a daemon I suggest that you use the `nohup` command associated with `&` at the end.

```bash
sudo nohup clarus-proxy &
``` 

## Docker

Go to the [Docker directory](docker/) to get the full documentation on how to use CLARUS with Docker. You can also check out the [CLARUS repository](https://hub.docker.com/r/clarus/proxy/) on Docker Hub.

## Installation From Sources

### API & Model

```bash
git clone https://github.com/clarus-proxy/dataoperations-api.git
cd dataoperations-api
mvn install
cd -
git clone https://github.com/clarus-proxy/security-policy-model.git
cd security-policy-model
mvn install
cd -
git clone https://github.com/clarus-proxy/JSqlParser.git
cd JSqlParser
mvn install
cd -
````

### Data Operation Modules

```bash
git clone https://github.com/clarus-proxy/anonymization-module.git
cd anonymization-module/
mvn install
cd -
git clone https://github.com/clarus-proxy/paillier.git
cd paillier
mvn install
cd -
git clone https://github.com/clarus-proxy/homomorphicencryption-module.git
cd homomorphicencryption-module
mvn install
cd -
git clone https://github.com/clarus-proxy/encryption-module.git
cd encryption-module
mvn install
cd -
git clone https://github.com/clarus-proxy/splitting-module.git
cd splitting-module
mvn install
cd -
git clone https://github.com/clarus-proxy/searchableencryption-module.git
cd searchableencryption-module/SE_module
mvn install
cd -
````

### Compile & Build the Proxy

```bash
git clone https://github.com/clarus-proxy/proxy.git
cd proxy
mvn install
```

### Get a test policy and launch the proxy !

__Linux systems__

```bash
cd install
cp ../main/src/test/resources/patient_anonymisation.xml test.xml
java -Djava.ext.dirs=./ext-libs/ -jar ./libs/proxy-main-1.0.2-SNAPSHOT.jar -sp test.xml 127.0.0.1
```

__Windows systems__

```batch
cd install
copy ..\main\src\test\resources\patient_anonymisation.xml test.xml
java -Djava.ext.dirs=.\ext-libs\ -jar .\libs\proxy-main-1.0.2-SNAPSHOT.jar -sp test.xml 127.0.0.1
```
