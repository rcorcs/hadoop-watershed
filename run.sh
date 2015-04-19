#$YARN_HOME/bin/hadoop fs -mkdir /hws/bin
#$YARN_HOME/bin/hadoop fs -copyFromLocal hws.jar /hws/bin/hws.jar
#for f in $(find ./lib/ -name "*.jar"); do
#   $YARN_HOME/bin/hadoop fs -copyFromLocal $f /hws/bin/$(basename $f)
#done

CPFILES='hws.jar'
for f in $(find ./lib/ -name "*.jar"); do
   CPFILES=$CPFILES':'$f
done
export HADOOP_CLASSPATH=$CPFILES

LOAD="
  tachyonnetconsumer.xml
  tachyonnetproducer.xml
"
$HADOOP_HOME/bin/hadoop jar hws.jar hws.core.JobClient -zks master --load $LOAD
#$YARN_HOME/bin/hadoop jar hws.jar hws.core.JobClient -jar hdfs:///hws/bin/hws.jar $*
