FROM ringcentral/jdk:latest

RUN mkdir /kafka

COPY kafka_2.12-2.8.0/ /kafka/

ENV KAFKA_HEAP_OPTS="-Xmx400m -Xms400m"
