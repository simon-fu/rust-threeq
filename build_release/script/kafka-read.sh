
KAFKA_ADDR="192.168.147.50:9092"
MQTT_TOPIC="qevbf0-Jmlcqhudqlonrhshnm"
BEGIN_TIME="2021-11-08T18:41:50"
NUM="50"

./bin/rtools kafka-read --addr $KAFKA_ADDR --topic $MQTT_TOPIC --begin $BEGIN_TIME --num $NUM

