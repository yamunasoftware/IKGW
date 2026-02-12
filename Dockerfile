FROM alpine:3.23.3
USER root
WORKDIR /ikgw
RUN mkdir /ikgw/logs

RUN apk update && apk add openjdk17-jre-headless maven
COPY . .
RUN mvn clean package
CMD ["java", "-jar", "target/IKGW-0.0.1-SNAPSHOT.jar"]