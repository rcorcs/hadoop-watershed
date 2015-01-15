for i in {1..4}; do ssh slave$i rm /home/hadoop/rcor/yarn/*.out ; done
#$YARN_HOME/bin/hadoop dfs -rm -r /hws/bin
$YARN_HOME/bin/hadoop dfs -rm -r /hws/apps
