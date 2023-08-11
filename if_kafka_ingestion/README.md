For Linux
Open a terminal session into kafka's package that you downloaded and extracted. Looks like this: 

     $ cd /home/your_usr/kafka_whateverversionyouhave

Since Kafka is trying to become more independent from Zookeeper services, be up-to-date and use the following:
     
     $ KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
     $ bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties 
     $ bin/kafka-server-start.sh config/kraft/server.properties


Do not close the terminal until your experience with the project is done. 

Open another terminal session and setup Redis on by following the README.md in redis_configuration/README.md

