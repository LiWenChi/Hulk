#MapReduce脚本执行相关操作：
##查看指定日志的文件：
	hadoop fs -ls /rds/supervise/*/2018-07-13
##配置权限：
	chown hdfs. * -R
	su hdfs
##删除supervise文件/目录
	sudo -u hdfs hdfs dfs -rm -r /rds/supervise
##查看各个目录下指定日期的文件
	hdfs dfs -ls /rds/*/*/*
	hdfs dfs -ls /rds/*/*/2018-10-23/*
##执行jar进行离线批处理
	sudo -u hdfs hadoop jar /data/works/java/liwenchi/client_to_server/client_to_server.jar /data/supervise/2018-08-22/* /rds/supervise 2018-08-22

##定时任务配置
	vim /data/works/java/liwenchi/client_to_server/job_client_to_server.sh


##定时任务
	启动时间：05:00
	30 00 * * * /data/works/java/liwenchi/client_to_server/job_client_to_server.sh
	启动时间：02:30
	30 2 * * * /opt/report-new.sh

##监听查看端口
	netstat -anp|grep 10000 查看端口是否被监听
	ps -ef| grep 11520  查看进程信息

##模块打包


![](https://i.imgur.com/QnHoRre.png)

![](https://i.imgur.com/w5vzkAu.png)

![](https://i.imgur.com/XGcVZ7e.png)

![](https://i.imgur.com/EBol8rn.png)

![](https://i.imgur.com/3LgztL1.png)

![](https://i.imgur.com/Aj57AVP.png)
