# JDproject
京东金融分析（毕业项目）

项目背景

一、问题描述

⾦条是京东⾦融旗下的⼀款无抵押现⾦贷产品，申请⼈只需要在京东⾦条申请页⾯填写少量的个⼈信息即可申请现⾦贷款。在开展这类信贷业务的时候，除了要评估⽤户的风险之外，还需要预测⽤户的借款需求，只有尽可能的给有借款需求的⽤户分配合适的额度，才能最⼤限度的增加资⾦利⽤率，降低成本并增加收益。
本题⽬希望参赛者通过竞赛数据中的⽤户基本信息、在移动端的⾏为数据、购物记录和历史借贷信建议信贷需求分析系统。本题⽬中包含了各种维度的序列数据、品类交易数据，并希望学员利⽤所学的hadoop与spark⼤数据技术，按照要求完成系⼤数据系统设计、程序实现和可视化展⽰部分。

二、数据说明

我们提供了时间为2016-08-03到2016-11-30期间，用户在移动端的行为数据、购物记录和历史借贷信息，及11月的总借款金额。
数据集下载地址为：链接:https://pan.baidu.com/s/1nvOOmmt密码:e73r

1、文件信息	

文件名	数据内容

t_user.csv	用户信息表

t_order.csv	订单信息表

t_click.csv	点击信息表

t_loan.csv	借款信息表

t_loan_sum.csv	月借款总额表


2、数据字典

文件名	字段名	字段描述

t_user	

  uid	用户ID
  
	age	年龄段
  
	sex	性别
  
	active_date	用户激活日期
  
	limit	初始额度
  
  
t_order

  uid	用户ID
  
	buy_time	购买时间
  
	price	价格
  
	qty	数量
  
	cate_id	品类ID
  
	discount	优惠金额
  
  
t_click

  uid	用户ID
  
	click_time	点击时间
  
	pid	点击页面
  
	param	页面参数
  
  
t_loan

  uid	用户ID
  
	laon_time	借款时间
  
	loan_amount	借款金额
  
	plannum	分期期数
  
  
t_loan_sum

  uid	用户ID
  
	month	统计月份
  
	loan_sum	借款总额
