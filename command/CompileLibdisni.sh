#!/usr/bin/env bash

# disni-1.7]#
mvn -DskipTests package

# disni-1.7]#
cd libdisni/
# libdisni]#
./autoprepare.sh
./configure --with-jdk=/opt/java/jdk1.8.0_151
make
make install #Libraries have been installed in:/usr/local/lib