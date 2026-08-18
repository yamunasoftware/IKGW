FROM vegardit/graalvm-maven:21.0.2 AS builder
WORKDIR /main
COPY src ./src
COPY pom.xml .

RUN mvn -B dependency:go-offline
RUN mvn -B clean package
RUN native-image \
    --no-fallback \
    --enable-url-protocols=http,https \
    -jar target/*.jar \
    ikgw

FROM debian:trixie
WORKDIR /main
COPY --from=builder /main/ikgw /main/ikgw
ENTRYPOINT ["/main/ikgw"]