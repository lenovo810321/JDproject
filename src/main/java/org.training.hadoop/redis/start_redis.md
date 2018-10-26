# 启动redis服务
cd redis
src/redis-server ./redis.conf &

# 访问redis
src/redis-cli

# 查看数据
127.0.0.1:6379> HGETALL click+
127.0.0.1:6379> HGETALL uses::total