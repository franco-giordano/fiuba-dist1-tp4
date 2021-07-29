#!/bin/bash
docker build -f client/Dockerfile -t "client:latest" .
docker run -i --network=fiuba-dist1-tp4_testing_net -v ~/fiuba-dist1-tp4-sources:/sources -v "$(pwd)/persistance/client":/persistance -e  ID='usuarioRebelde' client
