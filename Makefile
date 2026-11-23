COMPOSE := docker compose
BROKER ?= broker-1
TOPIC ?= events
KAFKA := $(COMPOSE) exec -T $(BROKER) /opt/kafka/bin
BOOTSTRAP := --bootstrap-server localhost:9092

.PHONY: topics brokers quorum produce consume lag # kafka
.PHONY: kill-consumer rebalance stop start client-logs # kafka
.PHONY: up down ps logs config # docker
.PHONY: docker-up docker-down docker-ps docker-logs docker-config # docker
.PHONY: verify

up docker-up: config
	$(COMPOSE) up -d

config docker-config:
	$(COMPOSE) config --quiet

down docker-down:
	$(COMPOSE) down

ps docker-ps:
	$(COMPOSE) ps -a

logs docker-logs:
	$(COMPOSE) logs --tail=100 -f

topics:
	$(KAFKA)/kafka-topics.sh $(BOOTSTRAP) --describe | column -t

brokers:
	$(KAFKA)/kafka-broker-api-versions.sh $(BOOTSTRAP)

quorum:
	$(KAFKA)/kafka-metadata-quorum.sh $(BOOTSTRAP) describe --status

produce:
	$(KAFKA)/kafka-console-producer.sh $(BOOTSTRAP) --topic "$(TOPIC)" --command-property acks=all

consume:
	$(KAFKA)/kafka-console-consumer.sh $(BOOTSTRAP) --topic "$(TOPIC)" --from-beginning --formatter-property print.partition=true --formatter-property print.offset=true

lag:
	$(KAFKA)/kafka-consumer-groups.sh $(BOOTSTRAP) --describe --group events-processor

verify:
	@$(COMPOSE) run --rm --no-deps -T --entrypoint /verify consumer

kill-consumer:
	@worker=$$($(COMPOSE) ps -q consumer | head -n 1); \
	test -n "$$worker" && docker kill --signal=KILL "$$worker"

rebalance:
	@$(COMPOSE) logs --no-color --since 3m --timestamps consumer | grep -E 'assigned|revoked|processing events\['

stop start:
	@test -n "$(SERVICE)" || { echo 'specify SERVICE' >&2; exit 1; }
	$(COMPOSE) $@ $(SERVICE)

client-logs:
	$(COMPOSE) logs --since 2m --tail 10 producer consumer
