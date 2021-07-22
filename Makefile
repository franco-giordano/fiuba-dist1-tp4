SHELL := /bin/bash
PWD := $(shell pwd)

default: build

all:

docker-image:
	docker build -f ./client-manager/Dockerfile -t "client-manager:latest" .
	# docker build -f ./filter-q1/Dockerfile -t "filter-q1:latest" .
	# docker build -f ./fanout-matches/Dockerfile -t "fanout-matches:latest" .
	# docker build -f ./fanout-players/Dockerfile -t "fanout-players:latest" .
	# docker build -f ./exchanger/Dockerfile -t "exchanger:latest" .
	# docker build -f ./group-by-nodes-q2/Dockerfile -t "group-by-nodes-q2:latest" .
	# docker build -f ./filter-by-ladder/Dockerfile -t "filter-by-ladder:latest" .
	# docker build -f ./filter-matches-q3/Dockerfile -t "filter-matches-q3:latest" .
	# docker build -f ./filter-pro-players-q4/Dockerfile -t "filter-pro-players-q4:latest" .
	# docker build -f ./join-nodes-q3/Dockerfile -t "join-nodes-q3:latest" .
	# docker build -f ./group-by-nodes-q3/Dockerfile -t "group-by-nodes-q3:latest" .
	# docker build -f ./filter-matches-q4/Dockerfile -t "filter-matches-q4:latest" .
	# docker build -f ./join-nodes-q4/Dockerfile -t "join-nodes-q4:latest" .
	# docker build -f ./group-by-nodes-q4/Dockerfile -t "group-by-nodes-q4:latest" .
	# docker build -f ./final-accumulator-q3/Dockerfile -t "final-accumulator-q3:latest" .
	# docker build -f ./final-accumulator-q4/Dockerfile -t "final-accumulator-q4:latest" .
	docker build -f ./master/Dockerfile -t "master:latest" .
.PHONY: docker-image

rabbit-up:
	docker-compose -f docker-compose-rabbit.yaml up -d --build
.PHONY: rabbit-up

rabbit-down:
	docker-compose -f docker-compose-rabbit.yaml stop -t 10
	docker-compose -f docker-compose-rabbit.yaml down
.PHONY: rabbit-down

rabbit-logs:
	docker-compose -f docker-compose-rabbit.yaml logs -f
.PHONY: rabbit-logs

nodes-up: docker-image
	docker-compose -f docker-compose-dev.yaml up -d --build
.PHONY: nodes-up

nodes-down:
	docker-compose -f docker-compose-dev.yaml stop -t 10
	docker-compose -f docker-compose-dev.yaml down
.PHONY: nodes-down

nodes-logs:
	docker-compose -f docker-compose-dev.yaml logs -f
.PHONY: nodes-logs
