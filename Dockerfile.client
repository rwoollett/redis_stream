# The new base image to contain runtime dependencies
# Dependencies: Boost >= 1.71


FROM debian:12.10 AS base

RUN apt update -y;  
RUN apt install -y build-essential cmake git openssl libssl-dev zlib1g-dev;

FROM base AS builder

WORKDIR /usr/src

# Install Boost 1.86 or later
COPY boost_1_86_0.tar.gz /usr/src

RUN BOOST_VERSION=1.86.0; \
    BOOST_DIR=boost_1_86_0; \
    tar -xvf boost_1_86_0.tar.gz; \
    cd ${BOOST_DIR}; \
    ./bootstrap.sh --prefix=/usr/local; \
    ./b2 --with-headers --with-system --with-thread --with-date_time --with-regex --with-serialization install; \
    cd ..; \
    rm -rf ${BOOST_DIR} ${BOOST_DIR}.tar.gz

COPY . /usr/src

# Build the project using CMake
RUN mkdir -p build; \
    cd build; \
    cmake .. -DBUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release
RUN cd build; make ClientRedis
RUN cd build; make ClientPublish
RUN cd build; make install

RUN strip /usr/local/bin/ClientRedis
# RUN strip /usr/local/bin/ClientPublish

FROM base AS runtime

COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /usr/local/lib /usr/local/lib

EXPOSE 8888
CMD ["ClientRedis"]
