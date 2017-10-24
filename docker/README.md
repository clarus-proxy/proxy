## CLARUS - Minimal Docker image

This image of a minimal CLARUS proxy is intended to be used to test security policies and CLARUS configuration. It doesn't suit a production environment since no optimization have been done.

## Image contents

* OpenJDK JRE 6
* MongoDB
* Clarus Proxy (version matching the Docker image tag)

## Usage

This image gives you a minimal installation for testing purposes.

Create a container using `clarus/proxy` image by doing 
```bash
docker run -d -p 5432:5432 --name <container-name> clarus/proxy:1.0.1
````
Replace the 5432 port (which is the default port for postgres) if it's already used in your environment. Replace 1.0.1 with the current Docker image tag that your are using.

By default the endpoint cloud ip (eg: the postgres server running in the cloud) is defined in the container to be accessible at the IP 127.0.0.1 and the port 5432. You need to change it so it can applied to your environment. To do so you need to override the docker environment variable `CLOUD_IP` and `CLOUD_PORT`


```bash
docker run -d -p 5432:5432 -e CLOUD_IP="<your_postgres_instance_cloud_ip_adress>" -e CLOUD_PORT="<your_postgres_instance_cloud_port>" --name <container-name> clarus/proxy:1.0.1
````

By default the security policy used for this container is a minimal security policy hosted publicly on [Github](https://github.com/clarus/proxy/tree/master/docker/configuration/security-policy-sample.xml). You need to change it by your own. To do so you need to override the environment variable `SECURITY_POLICY` and use your own security policy store in a Docker volume attached to the container.

```bash
docker run -d -p 5432:5432 -e SECURITY_POLICY="/etc/clarus/security-policy/<security-policy-file-name>" -v <path-to-your-directory>:/etc/clarus/security-policy/ --name <container-name> clarus/proxy:1.0.1
````

## User feedback

### Documentation

All the information regarding the Dockerfile is hosted publicly on [Github](https://github.com/clarus-proxy/proxy/tree/master/docker).

### Issues

If you find any issue with this image, feel free to report at [Github issue tracking system](https://github.com/clarus-proxy/proxy/issues).
