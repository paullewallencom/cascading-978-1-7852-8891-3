#!/bin/bash
# Run a hadoop job. $1 is the name of thee Hadoop job, $2-$5 are command line parameters
set -x
#
# Run TestFilterJob
#
scripts/runhadoop.sh TestFilterJob filtdata filtout
#
# Run TestCascadeJob
scripts/runhadoop.sh TestCascadeJob cascdata cascout1 cascout2
#
# Run TestAsserionJob
# note that you should get failures where assert conditions are not met
scripts/runhadoop.sh TestAssertionJob assertdata assertout
#
# Run TestAggregatorJob
scripts/runhadoop.sh TestAggregatorJob aggdata aggout
#
# Run TestNLPNamedBufferJob
scripts/runhadoop.sh TestNLPNameBufferJob ch8data nlpnameout
#
# Run TestNLPRelationshipJob
scripts/runhadoop.sh TestNLPRelationshipsJob ch8data nlprel1 nlprel2
#
# Run TestNLPSentAndTokenJob
scripts/runhadoop.sh TestNLPSentAndTokenJob ch8data nlpsenttok
#
# Run TestNLPSubassemblyJob
scripts/runhadoop.sh TestNLPSubassemblyJob ch8data nlpsub
#
# Run IntegrationTestob
scripts/runhadoop.sh IntegrationTestJob intdata intout1 intout2 intout3
#
#
#Run Chapter 8 code test
scripts/runhadoop.sh Chapter8JobHadoop ch8data ch8out1 ch8out2 ch8out3

echo "Done"
