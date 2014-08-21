#!/bin/bash

#
# (C) 2010-2012 ICM UW. All rights reserved.
#

mvn clean install -P sep -DskipTests
mkdir lib
cp target/*.jar lib
