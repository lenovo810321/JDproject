/*启动presto依赖服务*/

#启动依赖服务

##启动dfs服务
/home/zkpk/hadoop-2.7.3/sbin/start-dfs.sh
##启动mysql服务
sudo service mysqld start
##启动hive metastore服务
nohup hive --service metastore >> /home/zkpk/hive/metastore.log 2>&1 &

#启动presto服务
/home/zkpk/presto/bin/launcher start

#presto连接hive数据库
presto --server master:8080 --catalog hive --schema jd

