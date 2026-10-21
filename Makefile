# Makefile - entry points for the agent-memory demo on PostgreSQL (docs/PORT-CONTRACT.md section 8).
# Every target runs a script in scripts/; the scripts are idempotent.
#
#   make up                      run 00 to 60 in order
#   make chat MSG="Hi"           talk to the agent (NEW=1 starts a fresh conversation)
#   make teardown-all FORCE=1    remove everything this demo created, without the confirmation prompt

SHELL := /usr/bin/env bash
SCRIPTS := scripts
MSG ?=
NEW ?=

.DEFAULT_GOAL := help

.PHONY: help preflight setup-db setup-redpanda pipelines pipelines-local \
        deploy-agent verify up chat observe overdraft purge reset teardown teardown-all lint test

help:
	@echo "Agent memory on PostgreSQL (GantrySQL), governed by Redpanda"
	@echo ""
	@echo "  make preflight        check tools and logins, create .env from .postgres.env, verify connectivity"
	@echo "  make setup-db         PostgreSQL schema agent_memory, change-capture triggers and seed data"
	@echo "  make setup-redpanda   SASL user, ACLs, topics, secrets"
	@echo "  make pipelines        deploy the four Redpanda Connect pipelines (cloud): cdc, audit, observer, purge"
	@echo "  make pipelines-local  run the pipelines locally with rpk connect streams (ONLY=\"pg-agent-memory-cdc ...\" to pick)"
	@echo "  make deploy-agent     LLM providers (Anthropic + Bedrock), guardrail, MCP server, policies, agent"
	@echo "  make verify           end-to-end checks (PASS/FAIL lines)"
	@echo "  make up               all of the above, in order"
	@echo "  make chat MSG=\"...\"   send a message to the agent (NEW=1 for a new conversation);"
	@echo "                        the message is passed verbatim (dollar signs and quotes are fine)"
	@echo "  make observe          tail the CDC topic and poll the audit table"
	@echo "  make overdraft        drive account 1001 overdrawn (event-driven memory)"
	@echo "  make purge            delete expired memories now (what the hourly purge pipeline does)"
	@echo "  make reset            undo the overdraft and the observation memories (ALL=1: delete every demo-user memory)"
	@echo "  make teardown         remove the ADP agent, policies, MCP server and the pipelines"
	@echo "  make teardown-all     also topics, ACLs, user, secrets and the PostgreSQL schema (PROVIDERS=1: shared providers too)"
	@echo "  make lint             bash -n, rpk connect lint, shellcheck (if installed), MCP dry-run"
	@echo "  make test             rpk connect test for the pipeline mappings"

preflight:
	bash $(SCRIPTS)/00-preflight.sh

setup-db:
	bash $(SCRIPTS)/10-postgres-setup.sh

setup-redpanda:
	bash $(SCRIPTS)/20-redpanda-setup.sh

pipelines:
	bash $(SCRIPTS)/40-pipelines.sh

# make pipelines-local ONLY="pg-agent-memory-cdc pg-agent-memory-audit"
pipelines-local:
	bash $(SCRIPTS)/40-pipelines.sh --local $(ONLY)

deploy-agent:
	bash $(SCRIPTS)/50-deploy-agent.sh

verify:
	bash $(SCRIPTS)/60-verify.sh

# Explicit sequence (not prerequisites) so the order survives make -j.
up:
	$(MAKE) preflight
	$(MAKE) setup-db
	$(MAKE) setup-redpanda
	$(MAKE) pipelines
	$(MAKE) deploy-agent
	$(MAKE) verify

# make chat MSG="I prefer short answers"; without MSG the script reads stdin.
# $(value MSG) is the unexpanded text (so "$100" is not eaten by make) and it is
# piped to chat.sh inside single quotes (so the shell does not expand or re-quote
# it either); chat.sh reads the message from stdin when it gets no argument.
chat:
	@if [ -n '$(subst ','\'',$(value MSG))' ]; then \
	  printf '%s' '$(subst ','\'',$(value MSG))' | bash $(SCRIPTS)/chat.sh $(if $(NEW),--new,); \
	else \
	  bash $(SCRIPTS)/chat.sh $(if $(NEW),--new,); \
	fi

observe:
	bash $(SCRIPTS)/observe.sh

overdraft:
	bash $(SCRIPTS)/demo-overdraft.sh

purge:
	bash $(SCRIPTS)/demo-purge.sh

# make reset ALL=1 also deletes every memory of the demo user (true cold start).
reset:
	bash $(SCRIPTS)/demo-reset.sh $(if $(ALL),--all,)

teardown:
	bash $(SCRIPTS)/90-teardown.sh

# make teardown-all PROVIDERS=1 also removes the LLM providers and the guardrail
# (shared with the CockroachDB demo on the same ADP environment).
teardown-all:
	bash $(SCRIPTS)/90-teardown.sh --all $(if $(PROVIDERS),--providers,)

lint:
	bash -n $(SCRIPTS)/*.sh
	rpk connect lint --skip-env-var-check connect/*.yaml
	@if command -v shellcheck >/dev/null 2>&1; then \
	  shellcheck -x $(SCRIPTS)/*.sh; \
	else \
	  echo "shellcheck not installed; skipping"; \
	fi
	rpk ai mcp-server create pg-agent-memory -f adp/mcp-server.yaml --dry-run

test:
	rpk connect test connect/tests/*.yaml
