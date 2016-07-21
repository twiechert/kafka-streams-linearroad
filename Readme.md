## Container based deployment

- (optional) delete existing containers
  ```
    docker rm --force `docker ps -qa`
   ```

- (optional) it is generally a good idea to run the containers in a dedicated subnet
  ```
    docker network create --subnet=172.18.0.0/16 kafka-net
   ```


- create three containers using:
  ```
    docker run -d -P --name kafka-01 --net kafka-net --ip 172.18.0.22 ssh-ubuntu
    docker run -d -P --name kafka-02 --net kafka-net --ip 172.18.0.23 ssh-ubuntu
    docker run -d -P --name kafka-03 --net kafka-net --ip 172.18.0.24 ssh-ubuntu
    ```

 docker run -d -P --name kafka-01 twiechert/kafka-node1 'sudo service kafka-server start; sudo service zookeeper-server start;'

docker exec $(docker ps -aq) sudo service kafka-server start

- inspect the machine'S ip addresses using:

  ```
    docker inspect -f '{{.Name}} - {{.NetworkSettings.IPAddress }}' $(docker ps -aq)
   ```
- if the ip addresses in `ansible-kafka/inventory/static` differ from that output, adjust them accordingly...

- run the ansible playbook you find [here](https://github.com/twiechert/ansible-kafka/tree/docker_compatible)

  ```
    ansible-playbook -v -f 20 -i inventory/static playbooks/kafka.yml
    ```

- test to ssh the machine. In order to do that analyze the generated port mapping exetung `docker ps`  use username `root` and password `screencast`:

    ```
    ssh root@localhost -p 32788
    ```

- check then if the kafka and zookeeper services run by executing

 ```
zkCli.sh -cmd ls /brokers/ids
zkCli.sh -cmd get /brokers/ids/1
   ```


 ```
 bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
 bin/kafka-topics.sh --list --zookeeper localhost:2181
 bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
   ```



- (optional) stop existing containers

  ```
    docker stop `docker ps -qa`
  ```

- (optional) you can run commands in the containter directly
    ```
  docker exec kafka-01 /bin/bash -c "service zookeeper-server start; service kafka-server start";
  docker exec kafka-02 /bin/bash -c "service zookeeper-server start; service kafka-server start";
  docker exec kafka-03 /bin/bash -c "service zookeeper-server start; service kafka-server start";

  docker exec kafka-01 zkCli.sh -cmd ls /brokers/ids
    ```
## Run the benchmark

Simply start the main method located in `LinearRoadKafkaBenchmark` and pass the the input path to the raw data file (e.g. `--data-path=/home/tafyun/IdeaProjects/linearroad-java-driver/src/main/resources/datafile20seconds.dat`)
and the kafka nodes (e.g. `--bootstrap-servers=172.17.0.2:9092, 172.17.0.3:9092, 172.17.0.4:9092`) as programm arguements.


## Misc

- list current Kafka topics: kafka-topics.sh --zookeeper localhost:2181 --list

kafka-topics.sh --zookeeper localhost:2181 --delete --topic test