mkdir classes
YARNCP=$YARN_HOME/share/hadoop/yarn/hadoop-yarn-common-2.2.0.jar
YARNCP=$YARNCP:$YARN_HOME/share/hadoop/yarn/hadoop-yarn-api-2.2.0.jar
YARNCP=$YARNCP:$YARN_HOME/share/hadoop/yarn/hadoop-yarn-client-2.2.0.jar
YARNCP=$YARNCP:$YARN_HOME/share/hadoop/common/hadoop-common-2.2.0.jar
YARNCP=$YARNCP:$YARN_HOME/share/hadoop/common/lib/commons-logging-1.1.1.jar
YARNCP=$YARNCP:$YARN_HOME/share/hadoop/common/lib/commons-cli-1.2.jar
YARNCP=$YARNCP:$YARN_HOME/share/hadoop/common/lib/commons-io-2.1.jar
YARNCP=$YARNCP:$YARN_HOME/share/hadoop/common/lib/commons-codec-1.4.jar
YARNCP=$YARNCP:$YARN_HOME/share/hadoop/common/lib/commons-lang-2.5.jar
YARNCP=$YARNCP:$YARN_HOME/share/hadoop/hdfs/hadoop-hdfs-2.2.0.jar

CPFILES=$YARNCP
for f in $(find ./lib/ -name "*.jar"); do
   CPFILES=$CPFILES':'$f
done

JFILES=''
for f in $(find ./src/hws/ -name "*.java"); do
   JFILES=$JFILES' '$f
done

$YARN_HOME/bin/hadoop dfs -rm -r /hws/bin

javac -cp .:$CPFILES -d ./classes/ $JFILES
jar cf ./hws.jar -C classes/ .
rm -r classes

$YARN_HOME/bin/hadoop fs -mkdir /hws/bin
$YARN_HOME/bin/hadoop fs -copyFromLocal hws.jar /hws/bin/hws.jar
for f in $(find ./lib/ -name "*.jar"); do
   $YARN_HOME/bin/hadoop fs -copyFromLocal $f /hws/bin/$(basename $f)
done

