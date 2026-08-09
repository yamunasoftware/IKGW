FROM ubuntu:noble
WORKDIR /ikgw
RUN apt-get update && apt-get install -y openjdk21-jre-headless maven
COPY . .
RUN mvn clean package
CMD ["java", "-Xmx256m", "-jar", "target/IKGW-1.0.0.jar"]