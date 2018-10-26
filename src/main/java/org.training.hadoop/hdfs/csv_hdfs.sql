/*将t_load,t_click，t_loan和t_loan_sum导入到HDFS中*/

#在hdfs中创建/project/JD/data目录
hadoop fs -mkdir -p /project/JD/data
hadoop fs -mkdir /project/JD/data/t_click
hadoop fs -mkdir /project/JD/data/t_loan
hadoop fs -mkdir /project/JD/data/t_loan_sum

#上传文件
hadoop fs -put /home/zkpk/project/JD/data/t_click.csv /project/JD/data/t_click
hadoop fs -put /home/zkpk/project/JD/data/t_loan.csv /project/JD/data/t_loan
hadoop fs -put /home/zkpk/project/JD/data/t_loan_sum.csv /project/JD/data/t_loan_sum
