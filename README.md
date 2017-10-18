# Clarus Proxy
[![Build Status](https://travis-ci.org/clarus-proxy/proxy.svg?branch=master)](https://travis-ci.org/clarus-proxy/proxy)
The proxy component

## Pre Requisites

* Git
* OpenJDK >= 8.0
* Maven 3
* Gradle

## Installation

### Install API & Model

```bash
git clone https://github.com/clarus-proxy/dataoperations-api.git
cd dataoperations-api
git checkout develop
mvn install
cd -
````

```bash
git clone https://github.com/clarus-proxy/security-policy-model.git
cd security-policy-model
mvn install
cd -
````

```bash
git clone https://github.com/clarus-proxy/JSqlParser.git
cd JSqlParser
git checkout develop # In order to get the CLARUS version which was Patched
mvn install
cd -
````

### Install and compile the Data Operation Modules

```bash
git clone https://github.com/clarus-proxy/anonymization-module.git
cd anonymization-module/
git checkout develop
mvn install
cd -
````

```bash
git clone https://github.com/clarus-proxy/paillier.git
cd paillier
mvn install
cd -
````

```bash
git clone https://github.com/clarus-proxy/homomorphicencryption-module.git
cd homomorphicencryption-module
mvn install
cd -
````

```bash
git clone https://github.com/clarus-proxy/encryption-module.git
cd encryption-module
git checkout develop
mvn install
cd -
````

```bash
git clone https://github.com/clarus-proxy/splitting-module.git
cd splitting-module
git checkout develop
mvn install
cd -
````

```bash
git clone https://github.com/clarus-proxy/searchableencryption-module.git
cd searchableencryption-module/SE_module
git checkout develop
mvn install
cd -
````

### Compile & Build the Proxy
```bash
git clone https://github.com/clarus-proxy/proxy.git
cd proxy
git checkout develop
mvn install
cd -
````
