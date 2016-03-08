#!/bin/bash

kill -9 $(<work/spark.pid)
rm -rf work/checkpoint/

