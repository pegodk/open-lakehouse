FROM eclipse-temurin:17-jre-jammy

ARG UC_VERSION=0.2.0

RUN apt-get update && apt-get install -y curl unzip && rm -rf /var/lib/apt/lists/*

# Download and install Unity Catalog OSS
RUN curl -fsSL https://github.com/unitycatalog/unitycatalog/releases/download/v${UC_VERSION}/unitycatalog-${UC_VERSION}.tar.gz \
    -o /tmp/uc.tar.gz && \
    mkdir -p /opt/unitycatalog && \
    tar -xzf /tmp/uc.tar.gz -C /opt/unitycatalog --strip-components=1 && \
    rm /tmp/uc.tar.gz

WORKDIR /opt/unitycatalog

EXPOSE 8080

ENTRYPOINT ["bin/start-uc-server"]
