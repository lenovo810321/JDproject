/*将t_user用户信息导入MySQL中*/

#登陆MySQL
mysql -uroot -p123456

#创建数据库
create database jd;

#使用jd数据库
use jd;

#创建t_user表
create table if not exists t_user(
uid VARCHAR(50),
age INT,
sex VARCHAR(50),
active_date DATE,
`limit` DOUBLE,
PRIMARY KEY (uid ))
ENGINE=InnoDB DEFAULT CHARSET=utf8;

注：limit是关键字，要用用尖引号括起来，`limit`

#导入数据
>load data local 
infile "/home/zkpk/project/JD/data/t_user.csv" 
into table t_user 
fields terminated by ","  
lines terminated by "\r\n" 
ignore 1 lines;

#查看前10条数据
select * from t_user limit 10;