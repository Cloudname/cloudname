# The main service -- pacservice
FROM java:8
EXPOSE 4567:80
LABEL description="The demo service"
RUN mkdir -p /usr/local/lib/pacservice
ADD target/pacserver.jar /usr/local/lib/pacservice/pacserver.jar
ENV cloudname=zookeeper://localhost:2181
ENTRYPOINT exec /usr/bin/java \
	   -Djava.util.logging.SimpleFormatter.format='%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$-6s %2$s %5$s%6$s%n' \
	   -jar /usr/local/lib/pacservice/pacserver.jar \
	   --cloudname-url=${cloudname}

