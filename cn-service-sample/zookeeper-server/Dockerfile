# ZooKeeper test service. Exposes port 2181
FROM java:8
EXPOSE 2181:2181
RUN mkdir -p /usr/local/lib/zookeeper && mkdir -p /var/run/zookeeper
ADD target/cn-zookeeper.jar /usr/local/lib/zookeeper/cn-zookeeper.jar
LABEL description="The ZooKeeper test service"
ENTRYPOINT ["/usr/bin/java", "-jar", "/usr/local/lib/zookeeper/cn-zookeeper.jar"]
