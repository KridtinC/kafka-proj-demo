topic=$1
total_partition=$2

docker exec -d kafka-proj-demo-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic $topic --partitions $total_partition