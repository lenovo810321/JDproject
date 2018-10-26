# 提交主题t_click消费数据
bin/spark-submit --master local --class org.training.spark.streaming.JdUserClickCountAnalytics /home/zkpk/aura/jd3-1.0-SNAPSHOT-jar-with-dependencies.jar

# 提交主题t_order消费数据
bin/spark-submit --master local --class org.training.spark.streaming.JavaJdOrderSumAnalytics /home/zkpk/aura/jd3-1.0-SNAPSHOT-jar-with-dependencies.jar