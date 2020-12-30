#!/bin/bash

echo "filter"
hdfs dfs -cat filtout/p*

echo "cascade"
hdfs dfs -cat cascout*/p*

echo "asssert"
hdfs dfs -cat assertout/p*

echo "aggregator"
hdfs dfs -cat aggout/p*

echo "name bufffer"
hdfs dfs -cat nlpnameout/p*

echo "relationship"
hdfs dfs -cat nlprel*/p*

echo "sentence and token"
hdfs dfs -cat nlpsenttok/p*

echo "nlp subassembly"
hdfs dfs -cat nlpsub/p*

echo "inergraton test"
hdfs dfs -cat intout*/p*

echo "chapter8"
hdfs dfs -cat ch8*/p*
