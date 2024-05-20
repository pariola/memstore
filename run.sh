#!/bin/bash

function build_image() {
    docker build -t memstore .
}

function up() {
    while getopts "b" arg; do
        case "$arg" in
        b) BUILD_IMAGE="1" ;;
        *) ;;
        esac
    done

    if [[ $BUILD_IMAGE = "1" ]]; then
        build_image
    fi

    echo "starting docker compose"
    docker-compose up
}

# Set the command we want to run and remove the command part from the
# parameters. Only shift the parameters if there is something to shift.
command="$1"
[[ "$#" != "0" ]] && shift

case "$command" in
  up) up $* ;;
  build-image) build_image $* ;;
  *)
    echo "wth!"
esac