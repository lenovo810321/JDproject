/*将hive表查询结果保存到hdfs*/

bin/presto --execute "select age,by_time,sum(qty*price-discount) sum from hive.jd.t_order join hive.jd.t_user on t_order.uid = t_user.uid group by age,buy_time order by age, buy_time" --output-format CSV > /home/zkpk/project/JD/age_day_sum.csv
bin/presto --execute "select sex,loan_time,sum(loan_amount) sum from hive.jd.t_loan join hive.jd.t_user on t_loan.uid = t_user.uid group by loan_time, sex order by loan_time, sex" --output-format CSV > /home/zkpk/project/JD/sex_day_sum.csv