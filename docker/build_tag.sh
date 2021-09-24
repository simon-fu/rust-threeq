#!/bin/bash

# changable vars
GIT_BRANCH="v0.1.0"


GIT_REFS="tags/$GIT_BRANCH"
IMAGE_TAG="$GIT_BRANCH"
BUILD_PLATFORM="linux/amd64"

CMD="docker build \
--platform=$BUILD_PLATFORM \
--build-arg GIT_BRANCH=$GIT_BRANCH \
--build-arg GIT_REFS=$GIT_REFS \
--build-arg http_proxy=$http_proxy \
--build-arg https_proxy=$https_proxy \
--build-arg all_proxy=$all_proxy \
-t rust-threeq:$IMAGE_TAG -f ./Dockerfile . "

echo CMD=$CMD
$CMD
echo CMD=$CMD



