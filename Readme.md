## Container based deployment

- (optional) delete existing containers
  ```
    docker rm --force `docker ps -q`
   ```
- create three containers using:
  ```
    docker run -d -P --name kafka-01 ssh-ubuntu
    docker run -d -P --name kafka-02 ssh-ubuntu
    docker run -d -P --name kafka-03 ssh-ubuntu
    ```

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

## Run the benchmark

Simply start the main method located in `LinearRoadKafkaBenchmark` and pass the the input path to the raw data file (e.g. `--data-path=/home/tafyun/IdeaProjects/linearroad-java-driver/src/main/resources/datafile20seconds.dat`)
and the kafka nodes (e.g. `--bootstrap-servers=172.17.0.2:9092, 172.17.0.3:9092, 172.17.0.4:9092`) as programm arguements.
