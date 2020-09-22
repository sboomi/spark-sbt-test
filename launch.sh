#!/bin/bash

sbt package
spark-submit --class "ReadTitanic" --master local[4] \
target/scala-2.12/*.jar > statuslog.txt 2> errorlog.txt
grep "exception" -in errorlog.txt 