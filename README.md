# zookeeper_kafka

### Setup
```
$> pip install tweepy
$> pip install kafka
$> cd ~/kafka_2.11-0.10.1.0
$> nohup bin/zookeeper-server-start.sh config/zookeeper.properties &
$> nohup bin/kafka-server-start.sh config/server.properties &
```


### Change twitter key
Change your twitter [app](https://apps.twitter.com/) keys
```
$> vim config.py
```
### Import alchemy key
```
$> vim alchemyapi.py <YourApiKey>
```
### Run kafka
Follow [wiki](https://github.com/micklinISgood/zookeeper_kafka/wiki/kafka-Mac-OSX)

### Run producer/consumer module in separate machine
#### Machine A
```
$> python tweetKafkaProducer.py <machine B's ip>:9092
ex: python tweetKafkaProducer.py 54.187.126.150:9092
```
#### Machine B
```
$> tweetKafkaConsumer.py
```

