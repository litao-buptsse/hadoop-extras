#!/bin/bash

mvn clean package

rm -f hadoop-extras.tar
rm -fr hadoop-extras
mkdir -p hadoop-extras
cp target/hadoop-extras-1.0-SNAPSHOT.jar hadoop-extras
cp bin/* hadoop-extras
tar -cvf hadoop-extras.tar hadoop-extras
rm -fr hadoop-extras
