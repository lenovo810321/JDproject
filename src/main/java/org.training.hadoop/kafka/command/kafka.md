# 创建主题topic
bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 --partitions 1 --topic t_click
bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 --partitions 1 --topic t_order

# 查看是否成功
bin/kafka-topics.sh --list --zookeeper master:2181


# 运行主题t_click生产数据
java -cp /home/zkpk/aura/jd3-1.0-SNAPSHOT-jar-with-dependencies.jar org.training.hadoop.kafka.ClickProducer
# 运行主题t_order生产数据
java -cp /home/zkpk/aura/jd3-1.0-SNAPSHOT-jar-with-dependencies.jar org.training.hadoop.kafka.JavaJDKafkaOrderEventProducer