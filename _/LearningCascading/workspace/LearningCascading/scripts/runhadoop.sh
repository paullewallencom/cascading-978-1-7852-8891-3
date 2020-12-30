#!/bin/bash
# Run a hadoop job. $1 is the name of thee Hadoop job, $2-$5 are command line parameters
set -x
hadoop jar joblib/learning-cascading-2.6.3.jar com.ai.learning.jobs.hadoop.$1 $2 $3 $4 $5

