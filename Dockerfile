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

FROM alpine:3.23
WORKDIR /main
RUN apk add --no-cache ca-certificates
COPY --from=builder /main/ikgw /main/ikgw
ENTRYPOINT ["/main/ikgw"]