/*分析(1)各年龄段消费者每日购买商品总价值*/

#查询
select age,buy_time,sum(qty*price-discount) total_price
from t_order 
join t_user 
on t_order.uid = t_user.uid
group by age, buy_time 
order by age, buy_time;