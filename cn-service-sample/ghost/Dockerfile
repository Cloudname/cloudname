# The clyde service
FROM java:8
LABEL description="Blinky, Inky, Pinky and Clyde"
RUN mkdir -p /usr/local/lib/ghost
ADD target/cn-ghost.jar /usr/local/lib/ghost/ghost.jar
# These are case sensitive. Nice suprise there.
ENV cloudname=zookeeper://localhost:2181
ENV cloudname_interface=localhost
ENV ghost=blinky
ENTRYPOINT exec /usr/bin/java -jar /usr/local/lib/ghost/ghost.jar \
      --service-name ${ghost} --cloudname-url=${cloudname}

