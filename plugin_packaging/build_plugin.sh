#!/bin/bash

current=`pwd`
mkdir -p /tmp/gdprSHARK/
cp -R ../gdprSHARK /tmp/gdprSHARK/
cp ../setup.py /tmp/gdprSHARK/
cp ../main.py /tmp/gdprSHARK
cp ../wordfilter.txt /tmp/gdprSHARK
cp ../loggerConfiguration.json /tmp/gdprSHARK
cp * /tmp/gdprSHARK/
cd /tmp/gdprSHARK/

tar -cvf "$current/gdprSHARK_plugin.tar" --exclude=*.tar --exclude=build_plugin.sh --exclude=*/tests --exclude=*/__pycache__ --exclude=*.pyc *
