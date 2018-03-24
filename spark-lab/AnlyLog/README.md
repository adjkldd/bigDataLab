AnlyLog
-------

an exercise coding in scala from https://www.shiyanlou.com/courses/825


Deploy
------

1. copy `access_20170504.log` to `/opt/resources`

2. start docker containing spark:

    sudo docker run -v /opt/resources:/opt/resources -it ubuntu-spark

execute above command 3 times. In one container, execute `sbin/start-master.sh`,
execute `sbin/start-slave.sh` in the others

3. in current directory, exec: `sbt package`

4. use `spark-submit` to execute this job in spark cluster
