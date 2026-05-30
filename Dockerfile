FROM ubuntu:noble
USER root
WORKDIR /ikgw
RUN apt-get update && apt-get install -y openjdk17-jre-headless maven
COPY . .
RUN mvn clean package
CMD ["java", "-jar", "target/IKGW-0.0.1-SNAPSHOT.jar"]