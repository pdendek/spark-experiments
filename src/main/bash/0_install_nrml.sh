#!/bin/bash

#
# (C) 2010-2012 ICM UW. All rights reserved.
#

mvn clean install -D skipTests
mkdir lib
cp target/*SNAPSHOT.jar lib
