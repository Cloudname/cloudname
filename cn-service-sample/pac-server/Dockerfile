# The main service -- pacservice
FROM java:8
EXPOSE 4567:80
LABEL description="The demo service"
RUN mkdir -p /usr/local/lib/pacservice
ADD target/pacserver.jar /usr/local/lib/pacservice/pacserver.jar
ENV cloudname=zookeeper://localhost:2181
ENTRYPOINT exec /usr/bin/java -jar /usr/local/lib/pacservice/pacserver.jar --cloudname-url=${cloudname}

