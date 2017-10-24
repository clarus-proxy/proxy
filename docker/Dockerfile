# Best practices for writing Dockerfiles:
# https://docs.docker.com/engine/userguide/eng-image/dockerfile_best-practices/

# Tips to do an unattended installation on Debian/Ubuntu: 
# http://www.microhowto.info/howto/perform_an_unattended_installation_of_a_debian_package.html
FROM debian

MAINTAINER CLARUS Team (romain.ferrari[AT]gmail.com)


ENV DEBIAN_FRONTEND noninteractive

# Proxy configuration (if you are building from behind a proxy)
# Next release of docker 1.9.0 should allow you to configure these by passing build-time arguments
# More info: https://github.com/docker/docker/issues/14634

#ENV http_proxy 'http://user:password@proxy-host:proxy-port'
#ENV https_proxy 'http://user:password@proxy-host:proxy-port'
#ENV HTTP_PROXY 'http://user:password@proxy-host:proxy-port'
#ENV HTTPS_PROXY 'http://user:password@proxy-host:proxy-port'

ENV CLARUS_DEB_DOWNLOAD_URL="https://github.com/clarus-proxy/proxy/releases/download/v1.0.1/clarus-proxy_1.0.1.deb"

# Where there is a command with a pipe, we need to put in between quotes and make it an argument to bash -c command
RUN apt-get update --assume-yes -qq && \
 apt-get install --assume-yes -qq \
        locales-all \ 
        locales \
        less \
        apt-utils \ 
        debconf-utils \
        gdebi \
        curl && \
rm -rf /var/lib/apt/lists/*

RUN locale-gen en_US en_US.UTF-8
RUN dpkg-reconfigure locales
ENV LANG en_US.UTF-8 
ENV LANGUAGE en_US:en 
ENV LC_ALL en_US.UTF-8 

RUN curl --silent --output authzforce-ce-server.deb --location $CLARUS_DEB_DOWNLOAD_URL && \
dpkg -i clarus-proxy_1.0.1.deb && \
rm -f clarus-proxy_1.0.1.deb
CMD ["/usr/bin/clarus-proxy", "/etc/clarus/clarus-proxy.conf", "/etc/clarus/clarus-keystore.conf"]

### Exposed ports
# - App server
EXPOSE 6543
