FROM docker-upstream.apple.com/curlimages/curl:7.80.0

USER root
RUN apk --no-cache add jq bash ca-certificates

ENV TERMINATION_MESSAGE_PATH /dev/termination-log
ENV CURL_STDERR_FILE_PATH /dev/curl_stderr

# Taken from https://github.pie.apple.com/crypto-services/trust-apple-corp-root-cas#alpine
# Configure trust for Apple Corporate Root CAs
ADD "https://pages.github.pie.apple.com/crypto-services/trust-apple-corp-root-cas/trust_apple_corp_root_cas.sh" "/usr/local/bin"
RUN chmod 755 "/usr/local/bin/trust_apple_corp_root_cas.sh" && \
    bash "/usr/local/bin/trust_apple_corp_root_cas.sh"

RUN pwd
COPY src/main/resources/user-dependencies/download-dependencies.sh /opt/
RUN chmod +x /opt/download-dependencies.sh

ENTRYPOINT [ "/bin/bash", "-c" ]
CMD ["/opt/download-dependencies.sh 2> >(tee -a $TERMINATION_MESSAGE_PATH >&2)"]