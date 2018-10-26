/*男女消费者每日借贷金额*/

#查询
select sex, loan_time, sum(loan_amount) total_loan 
from t_loan 
join t_user 
on t_loan.uid = t_user.uid 
group by loan_time, sex 
order by loan_time, sex;