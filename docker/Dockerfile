FROM rust:1.52.1-buster as build

# display env
RUN echo HOME=$HOME
RUN echo USER=$USER

# create a new empty shell project
WORKDIR /
RUN USER=root cargo new --bin work
WORKDIR /work

# config cargo with new source
RUN mkdir -p ./.cargo
RUN touch ./.cargo/config.toml
RUN echo "[source.crates-io]" >> ./.cargo/config
RUN echo "replace-with = 'rsproxy'" >> ./.cargo/config
RUN echo "" >> ./.cargo/config
RUN echo "[source.rsproxy]" >> ./.cargo/config
RUN echo "registry = \"https://rsproxy.cn/crates.io-index\"" >> ./.cargo/config
RUN cat ./.cargo/config



# copy over your manifests
# COPY ./Cargo.lock ./Cargo.lock
COPY ./files/Cargo.toml ./Cargo.toml

# this build step will cache your dependencies
RUN cargo build --release


# clean src
RUN rm -rf /work/*


# git clone source code
# from https://stackoverflow.com/questions/36996046/how-to-prevent-dockerfile-caching-git-clone
ARG SRC_HOME=/work/repo
ARG GIT_USER=simon-fu
ARG GIT_REPO=rust-threeq
ARG GIT_BRANCH=main
ARG GIT_REFS=heads/main

ENV http_proxy $http_proxy
ENV https_proxy $https_proxy
ENV all_proxy $all_proxy

RUN echo before cloning
RUN echo http_proxy=$http_proxy https_proxy=$https_proxy all_proxy=$all_proxy

ADD https://api.github.com/repos/$GIT_USER/$GIT_REPO/git/refs/$GIT_REFS version.json
RUN git clone -b $GIT_BRANCH https://github.com/$GIT_USER/$GIT_REPO.git $SRC_HOME/ 2>&1
RUN cd $SRC_HOME && git checkout -b $GIT_BRANCH; exit 0

RUN unset http_proxy
RUN unset https_proxy
RUN unset all_proxy

RUN echo after cloning
RUN echo http_proxy=$http_proxy https_proxy=$https_proxy all_proxy=$all_proxy

# copy your source tree
# COPY ./src ./src
# COPY ./proto ./proto
# COPY ./benches ./benches
# COPY ./build.rs .
# COPY ./ ./

# build for release
RUN rm -rf $SRC_HOME/target/*
RUN cd $SRC_HOME && cargo build --release

# our final base
# FROM rust:1.52.1-slim-buster
FROM frolvlad/alpine-glibc:alpine-3.14_glibc-2.33
# FROM alpine:3.14

WORKDIR /work

# copy the build artifact from the build stage
COPY --from=build /work/repo/target/release/rtools ./
COPY --from=build /work/repo/target/release/rbench ./
COPY --from=build /work/repo/target/release/rbroker ./

ENV PATH="${PATH}:/work"

COPY ./files/docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh
EXPOSE 1883
VOLUME ["/work/data", "/work/log"]

ENTRYPOINT ["/work/docker-entrypoint.sh"]
CMD ["/work/rbroker"]

