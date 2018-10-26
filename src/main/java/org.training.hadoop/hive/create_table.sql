/*将t_user、t_order和t_loan表导入hive*/

#启动metastore服务
nohup hive --service metastore >> /home/zkpk/hive/metastore.log 2>&1 &

#进入hive-shell
hive

#创建数据库
create database jd;

#使用jd数据库
use jd；

#将t_user导入hive

create external table if not exists t_user (
uid STRING,
age INT,
sex CHAR(2),
active_date DATE,
`limit` FLOAT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
lines terminated by '\n'
stored as textfile
location '/project/JD/data/t_user'
;

#将t_order导入hive

##创建外部表
create external table if not exists t_order (
uid STRING, 
buy_time date, 
price double, 
qty int, 
cate_id int,
discount double) 
row format delimited 
fields terminated by ',' 
lines terminated by '\n' 
stored as textfile
tblproperties("skip.header.line.count"="1")
location '/project/JD/data/t_order/'
;
##导入数据
hadoop fs -mkdir /project/JD/data/t_order
hadoop fs -put /home/zkpk/project/JD/data/t_order.csv /project/JD/data/t_order

#将t_loan导入hive

create external table if not exists t_loan (
uid int, 
loan_time timestamp,
loan_amount double,
plannum int) 
row format delimited 
fields terminated by ',' 
lines terminated by '\n' 
stored as textfile
tblproperties("skip.header.line.count"="1")
location '/project/JD/data/t_loan/'
;