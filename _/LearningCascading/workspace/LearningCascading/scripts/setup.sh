#!/bin/bash
set -x

hdfs dfs -rm -r data
hdfs dfs -rm -r ch8data
hdfs dfs -rm -r aggdata
hdfs dfs -rm -r assertdata
hdfs dfs -rm -r cascdata
hdfs dfs -rm -r filtdata
hdfs dfs -rm -r intdata
hdfs dfs -rm -r models
hdfs dfs -rm -r dictionaries

hdfs dfs -rm -r filtout
hdfs dfs -rm -r cascout*
hdfs dfs -rm -r assertout
hdfs dfs -rm -r aggout
hdfs dfs -rm -r nlpnameout
hdfs dfs -rm -r nlprel*
hdfs dfs -rm -r nlpsenttok
hdfs dfs -rm -r nlpsub
hdfs dfs -rm -r intout*
hdfs dfs -rm -r ch8out*


hdfs dfs -mkdir data
hdfs dfs -mkdir ch8data
hdfs dfs -mkdir aggdata
hdfs dfs -mkdir assertdata
hdfs dfs -mkdir cascdata
hdfs dfs -mkdir filtdata
hdfs dfs -mkdir intdata
hdfs dfs -mkdir models
hdfs dfs -mkdir dictionaries
hdfs dfs -copyFromLocal data/Chapter8Data.txt ch8data
hdfs dfs -copyFromLocal data/TestAggregator.txt aggdata
hdfs dfs -copyFromLocal data/TestAssertion.txt assertdata
hdfs dfs -copyFromLocal data/TestCascade.txt cascdata
hdfs dfs -copyFromLocal data/IntegrationTest.txt intdata
hdfs dfs -copyFromLocal data/TestFilter.txt filtdata
hdfs dfs -copyFromLocal models/* models
hdfs dfs -copyFromLocal dictionaries/* dictionaries
echo "Done"
