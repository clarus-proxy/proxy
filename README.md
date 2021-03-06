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
mvn install
cd -
````

### Install and compile the Data Operation Modules

```bash
git clone https://github.com/clarus-proxy/anonymization-module.git
cd anonymization-module/
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
mvn install
cd -
````

```bash
git clone https://github.com/clarus-proxy/splitting-module.git
cd splitting-module
mvn install
cd -
````

```bash
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
java -Djava.ext.dirs=./ext-libs/ -jar ./libs/proxy-main-1.0.1.jar -sp test.xml 127.0.0.1
```

__Windows systems__

```batch
cd install
copy ..\main\src\test\resources\patient_anonymisation.xml test.xml
java -Djava.ext.dirs=.\ext-libs\ -jar .\libs\proxy-main-1.0.1.jar -sp test.xml 127.0.0.1
```


## License

All the data protection modules are being licensed under the Apache 2.0 License. The protocol module is available under the EUPL v1.2 license.
