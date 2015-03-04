mkdir classes
YARNCP=$HADOOP_HOME/share/hadoop/yarn/hadoop-yarn-common-2.6.0.jar
YARNCP=$YARNCP:$HADOOP_HOME/share/hadoop/yarn/hadoop-yarn-api-2.6.0.jar
YARNCP=$YARNCP:$HADOOP_HOME/share/hadoop/yarn/hadoop-yarn-client-2.6.0.jar
YARNCP=$YARNCP:$HADOOP_HOME/share/hadoop/common/hadoop-common-2.6.0.jar
YARNCP=$YARNCP:$HADOOP_HOME/share/hadoop/common/lib/commons-logging-1.1.3.jar
YARNCP=$YARNCP:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar
YARNCP=$YARNCP:$HADOOP_HOME/share/hadoop/common/lib/commons-io-2.4.jar
YARNCP=$YARNCP:$HADOOP_HOME/share/hadoop/common/lib/commons-codec-1.4.jar
YARNCP=$YARNCP:$HADOOP_HOME/share/hadoop/common/lib/commons-lang-2.6.jar
YARNCP=$YARNCP:$HADOOP_HOME/share/hadoop/hdfs/hadoop-hdfs-2.6.0.jar
YARNCP=$YARNCP:$HADOOP_HOME/share/hadoop/tools/lib/commons-math3-3.1.1.jar

CPFILES=$YARNCP:./hws.jar
for f in $(find ./lib/ -name "*.jar"); do
   CPFILES=$CPFILES':'$f
done

JFILES=''
for f in $(find ./src/sample/ -name "*.java"); do
   JFILES=$JFILES' '$f
done

javac -cp .:$CPFILES -d ./classes/ $JFILES
jar cf ./sample.jar -C classes/ .
rm -r classes
