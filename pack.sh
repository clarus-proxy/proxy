#!/bin/bash

# This line contains the repositories to clone
# IMPORTANT: The order of this array IS crucial in order to correctly compile
# each repository, since the last ones might depend on the others
REPOSITORIES=(JSqlParser dataoperations-api paillier anonymization-module encryption-module splitting-module searchableencryption-module proxy)

VERSION=1.0.1

# Tools to use
MAVEN_COMMAND=mvn
JAVAC_COMMAND=javac
GIT_COMMAND=git

# Deb file name
DEB_NAME=clarus-proxy_$VERSION

# Temporal folder to download, compile and pack
TEMP_CLARUS_FOLDER=clarus-sources

# Create the temporal folder
mkdir $TEMP_CLARUS_FOLDER
cd $TEMP_CLARUS_FOLDER

# Download and compile each repository
for REPOSITORY in ${REPOSITORIES[@]}
do
   # Get the code
   $GIT_COMMAND clone https://github.com/clarus-proxy/$REPOSITORY.git

   if [ $REPOSITORY == "searchableencryption-module" ]; then
      cd $REPOSITORY/SE_module
   else
      cd $REPOSITORY
   fi

   # Checkout the right release
   $GIT_COMMAND checkout tags/v"$VERSION"
   # Compile
   $MAVEN_COMMAND install

   # in case of any error, abort everything
   if [[ $? != 0 ]]; then
      echo "ERROR compiling $REPOSITORY";
      exit 1
   fi

   if [ $REPOSITORY == "searchableencryption-module" ]; then
      cd ../..
   else
      cd ..
   fi
done

# Create the Debian packaging folder
mkdir $DEB_NAME
mkdir -p $DEB_NAME/opt/clarus/
mkdir -p $DEB_NAME/etc/clarus/
mkdir -p $DEB_NAME/usr/bin/
mkdir -p $DEB_NAME/DEBIAN

# Copy the files
cp -R proxy/install/libs $DEB_NAME/opt/clarus
cp -R proxy/install/ext-libs $DEB_NAME/opt/clarus

# Copy executable and conf file
cp ../clarus-proxy $DEB_NAME/usr/bin/.
cp ../clarus-proxy.conf $DEB_NAME/etc/clarus/.
cp ../clarus-keystore.conf $DEB_NAME/etc/clarus/.

echo "Package: clarus-proxy" >> $DEB_NAME/DEBIAN/control
echo "Version: $VERSION">> $DEB_NAME/DEBIAN/control
echo "Section: base">> $DEB_NAME/DEBIAN/control
echo "Priority: standard">> $DEB_NAME/DEBIAN/control
echo "Architecture: all">> $DEB_NAME/DEBIAN/control
echo "Maintainer: CLARUS Consortium <diego.rivera@montimage.com>">> $DEB_NAME/DEBIAN/control
echo "Description: The CLARUS proxy. A data protection proxy.">> $DEB_NAME/DEBIAN/control
echo "Depends: openjdk-8-jdk (>= 8u77-b03-3ubuntu3), mongodb (>= 1:2.6.10-0ubuntu1)">> $DEB_NAME/DEBIAN/control

dpkg-deb -b $DEB_NAME
