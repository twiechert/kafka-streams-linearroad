## Description
This is an implementation of the [Linear Road benchmark](http://www.isys.ucl.ac.be/vldb04/eProceedings/contents/pdf/RS12P1.PDF) using Kafka
streams. A general discussion of the benchmark components and how they be realized, can be found [here](https://github.com/twiechert/linear-road-general).


Simply start the main method located in `LinearRoadKafkaBenchmark` and pass the the input path to the raw data file (e.g. `--data-path=/home/tafyun/IdeaProjects/linearroad-java-driver/src/main/resources/datafile20seconds.dat`)
and the kafka nodes (e.g. `linearroad.kafka.bootstrapservers=172.17.0.2:9092, 172.17.0.3:9092, 172.17.0.4:9092 --linearroad.zookeeper.server=172.17.0.2:2181`) as program arguments.
## Container based deployment

Quick and dirty setup
https://hub.docker.com/r/ches/kafka/

docker run -d \
    --hostname localhost \
    --name kafka \
    --publish 9092:9092 --publish 7203:7203 \
    --env KAFKA_ADVERTISED_HOST_NAME=127.0.0.1 --env ZOOKEEPER_IP=127.0.0.1 \ches/kafka



    zkCli.sh -cmd ls /brokers/ids
    zkCli.sh -cmd get /brokers/ids/1
      ```
