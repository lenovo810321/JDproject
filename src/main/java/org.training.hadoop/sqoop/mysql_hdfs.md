/*利用Sqoop将MySQL中的t_user表导入到HDFS中*/

默认创建4个分区

sqoop import --connect jdbc:mysql://master:3306/jd --username hadoop --password hadoop --table t_user --target-dir /project/JD/data/user