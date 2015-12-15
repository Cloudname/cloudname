FROM ubuntu:latest
MAINTAINER stalehd@telenordigital.com
CMD mkdir -p /consul/data
ADD consul_linux /consul/consul
ADD ./ui /usr/local/consul-ui
ENV interface=localhost
ENTRYPOINT /consul/consul agent -server -bootstrap-expect 1 -data-dir /consul/data -bind ${interface} -client ${interface} -ui-dir /usr/local/consul-ui


