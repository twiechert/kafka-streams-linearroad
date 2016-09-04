## Description
This is an implementation of the [Linear Road benchmark](http://www.isys.ucl.ac.be/vldb04/eProceedings/contents/pdf/RS12P1.PDF) using Kafka
streams. A general discussion of the benchmark components and how they be realized, can be found [here](https://github.com/twiechert/linear-road-general).


Simply start the main method located in `LinearRoadKafkaBenchmark` and pass the the input path to the raw data file (e.g. `--data-path=/home/tafyun/IdeaProjects/linearroad-java-driver/src/main/resources/datafile20seconds.dat`)
and the kafka nodes (e.g. `linearroad.kafka.bootstrapservers=172.17.0.2:9092, 172.17.0.3:9092, 172.17.0.4:9092 --linearroad.zookeeper.server=172.17.0.2:2181`) as program arguments.

### Starting the application
For most configurations there are default values specified which can be overiden as shown below. Because the cluster profile is set active,
the application-cluster.properties file has precedence and overrides the deault values specified in application.properties. If you execute the program as shown below, all stream tuples of the NOV and LAV
stream will be printed.


```
java -jar apps/kafka-streams-linearroad.jar \
--linearroad.data.path=/home/hadoop/linearroad/L1/car.dat \
--linearroad.hisotical.data.path=/home/hadoop/linearroad/L1/car.dat.tolls.dat \
--linearroad.kafka.bootstrapservers=localhost:9092 \
--linearroad.mode.debug=NOV,LAV
--spring.profiles.active=cluster
```

If the historical toll table has already been feed, you can skip this part by callind instead

```
java -jar apps/kafka-streams-linearroad.jar \
--linearroad.data.path=/home/hadoop/linearroad/L1/car.dat \
--linearroad.hisotical.data.path=/home/hadoop/linearroad/L1/car.dat.tolls.dat \
--linearroad.kafka.bootstrapservers=localhost:9092 \
--linearroad.mode=no-historial-feed
--linearroad.mode.debug=NOV,LAV
--spring.profiles.active=cluster
```


## Serdes
The benchmark is currently configured to use the [fast-serialization](https://github.com/RuedigerMoeller/fast-serialization) library, because in our conducted experiments in
the fastest and most space efficient library from those, we have tested. You may replace this library and this repository already integrates
Serde implementation for Jackson (with Smile Addon) and Kryio all located in `core.serde.provider`. In order to change the library system-wise, simply make
the class `core.serde.DefaultSerde` extent a different Serde implementation.

## L-Rating and validation Files
The files have been creates using [Linear Generator](https://github.com/walmart/LinearGenerator) and
 [linearroad](https://github.com/walmart/linearroad)

https://www.dropbox.com/s/ll8z934s6q278q5/L1.tar.gz?dl=1
https://www.dropbox.com/s/mmcinm55te2c94y/validate_l1.tar.gz?dl=1

https://www.dropbox.com/s/e22s5d2k9gpr3d5/L2.tar.gz?dl=1
https://www.dropbox.com/s/qifoz1hmklm8l2n/validate_l2.tar.gz?dl=1

https://www.dropbox.com/s/d0mlu2bqm4mal6f/L3.tar.gz?dl=1
https://www.dropbox.com/s/g6e0zhkyrhd0lxn/validate_l3.tar.gz?dl=1

https://www.dropbox.com/s/xssgj6tvyb4snrz/L4.tar.gz?dl=1


https://www.dropbox.com/s/rl4udvdugqmx583/L6.tar.gz?dl=1