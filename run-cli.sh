#!/bin/bash
docker build -f client/Dockerfile -t "client:latest" .
docker run -i -v "$(pwd)/results":/results --network=fiuba-dist1-tp2_testing_net client
