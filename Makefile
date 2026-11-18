COMPOSE := docker compose

.PHONY: up down ps logs config
.PHONY: docker-up docker-down docker-ps docker-logs docker-config

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
