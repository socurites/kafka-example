# 아파치 카프카 스터디 예제
## Book: 아파치 카프카 애플리케이션 프로그래밍 with 자바, 최원영 지음

## Local Env
Dockerfile

### run zookeeper
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties

### run kafka-server
bin/kafka-server-start.sh -daemon config/server.properties

### run connect
bin/connect-distributed.sh -daemon config/connect-distributed.properties
