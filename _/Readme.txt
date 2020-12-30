Learning Cascading Code
Copyright ©2015 Analytics Inside LLC

Included in this package is the code described in this book.  The following is included
1. The complete Eclipse workspace with the projects that includes both local and Hadoop mode
2. Necessary Hadoop and Cascading JAR files 
3. A FAT JAR file which is created from this code 
4. Bash shell scripts to run the code in Hadoop mode
5. Test data files
6. NLP Models and Dictionaries

************************************************************************
The code has been tested with Cascading 2.6.3, Hadoop 2.2 in CDH 5.3 environment 
on Linux Centos 6.4
*************************************************************************
Eclipse Workspace

The workspace is configured to work with Eclipse Juno or above. Java System Library must be JavaSE-1.7 or above.
After unzipping this zip file, open Eclipse and select File->Switch Workspace->other. Navigate and select the Workspace directory under Learning Cascading. 
The workspace with everything described above will be created in your Eclipse environment. 
Below are the descriptions of directories within Eclipse workspace. 
Note that if you do not use Eclipse, this directory structure is the same in your OS environment.

SRC  
Com.ai.learning package -contains all of the code for test Cascade Operations and utilities 
Com.ai.learning.jobs.local package contains all of the test jobs for local mode
Com.ai.learning.jobs.hadoop package contains all of the test jobs for Hadoop mode

DATA  - sample test data. Most of the data files are named similarly to the test jobs

MODELS - Open NLP models downloaded from http://opennlp.sourceforge.net/models-1.5/

DICTIONARIES - simple chemical dictionary created by Analytics Inside

LIB - JAR files which are used to build both local and Hadoop projects

JOBLIB - learning_cascading_2.6.3.jar created by us for running this code

SCRIPTS - bash shell scripts to run examples in Hadoop mode

OUTPUT - that is where output files are configured to be placed in local mode

Running Local Mode

You can run local mode examples from Eclipse. All of the inputs and outputs are pre-configured to run with the correct input and output files. 
You can use Run As option from Eclipse and just select Java Application. The correct configuration will be loaded automatically. For UnitTestJob select UnitTest.
If you want to examine or modify the configuration, select Run Configurations from Run As option and select a correct configuration to run. 
The run configurations are named the same as the jobs they are running. Select Arguments tab to examine inputs and outputs.
The output files will show up in the OUTPUT directory. You may want to refresh it after the run.

If you are not using Eclipse, here are the inputs and outputs for all test jobs. You can use joblib/learning_cascading_2.6.3.jar to run them

com.ai.learning.jobs.local.Chapter8JobLocal data/Chapter8Data.txt output/Chapter8Names.txt output/Chapter8Rel.txt output/Chapter8Counts.txt
com.ai.learning.jobs.local.IntegrationTestJob data/IntegrationTest.txt output/IntegrationTestNames.txt output/IntegrationTestRel.txt output/IntegrationTestCounts.txt
com.ai.learning.jobs.local.TestAggregatorJob data/TestAggregator.txt output/TestAggregatorOut.txt
com.ai.learning.jobs.local.TestAssertionJob data/TestAssertion.txt output/TestAssertionOut.txt
com.ai.learning.jobs.local.TestCascadeJob data/TestCascade.txt output/TestCascadeSent.txt output/TestCascadeTok.txt
com.ai.learning.jobs.local.TestFilterJob data/TestFilter.txt output/TestFilterOut.txt
com.ai.learning.jobs.local.TestNLPNameBufferJob data/Chapter8Data.txt output/TestNLPBufferOut.txt
com.ai.learning.jobs.local.TestNLPRelationshipsJob data/Chapter8Data.txt output/TestRelJobNames.txt output/TestRelJobRel.txt
com.ai.learning.jobs.local.TestNLPSentAndTokenJob data/Chapter8Data.txt output/TestSentTok.txt
com.ai.learning.jobs.local.TestNLPSubassemblyJob data/Chapter8Data.txt output/TestNLPSub.txt

Running Hadoop Mode

Unfortunately it is close to impossible to run Hadoop mode from Eclipse. So to test Hadoop mode code, it is better to run it from command line. 
Keep in mind that we are using HDFS file system for Hadoop mode, and therefore we must prepare the HDFS input directories.

We are providing shell scripts in the directory SCRIPTS to help you run code in Hadoop mode.

The scripts are:
Setup.sh  --- sets up HDFS 
Runhadoop.sh is a generic script to run a single job in Hadoop mode
Runhadoopall.sh runs all jobs in Hadoop mode
Seeresults.sh see the results of all Hadoop jobs

Run scripts/setup.sh from workspace/LearningCascading directory to clean up HDFS, to set up all the necessary directories, and to copy local data files into HDFS.  
You must run it BEFORE testing Hadoop mode code.

Run scripts/runhadoopall.sh from workspace/LearningCascading directory to run all of the examples. 
Please note that we are providing the project FAT JAR file in joblib/ learning_cascading_2.6.3.jar
Look at the code of Runhadoopall.sh it is commented and the code will show you how to run individual jobs as well

Run scripts/seeresults.sh from workspace/LearningCascading directory to see the results of the example programs. 
Look at the code of seeresultsl.sh it will show you how to see the results of an individual test job.


Good luck and we hope you enjoyed Leaning Cascading!


