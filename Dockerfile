ARG BASE_IMG=openjdk:17.0.2

FROM $BASE_IMG

ARG VERSION="UNKNOWN"
ARG BUILD_TIME="UNKNOWN"
ARG GIT_COMMIT="UNKNOWN"

# Switch to root user to install packages
USER root

WORKDIR /gateway

# Copy all files
COPY ./src ./src
RUN  sed -i -e "s/INJECT_VERSION/${VERSION}/g"  \
            -e "s/INJECT_GIT_COMMIT/${GIT_COMMIT}/g" \
            -e "s/INJECT_BUILD_TIME/${BUILD_TIME}/g" ./src/main/resources/version.txt
COPY ./build ./build
COPY ./pom.xml ./pom.xml
COPY ./entrypoint.sh /opt/entrypoint.sh

# Install packages
RUN microdnf install -y gzip
RUN microdnf install -y unzip && microdnf clean all
RUN curl https://downloads.apache.org/maven/maven-3/3.8.6/binaries/apache-maven-3.8.6-bin.zip -o apache-maven-3.8.6-bin.zip
RUN unzip apache-maven-3.8.6-bin.zip
RUN rm apache-maven-3.8.6-bin.zip

# Package
RUN apache-maven-3.8.6/bin/mvn clean package -Dmaven.test.skip

EXPOSE 8080 8080

RUN chmod 0755 /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]

CMD ["config.yml"]
