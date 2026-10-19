# Demo script: Memory Concierge (15 minutes)

Six acts, one agent, two terminals and the ADP console. Terminal A is for chat, terminal B runs `make observe` from Act 2 onward. Commands are exact; the "say" lines are a talk track, not a script to read aloud. `make chat MSG="..."` continues the current conversation and `make chat NEW=1 MSG="..."` starts a new one (the same as `scripts/chat.sh --new "..."`).

## Pre-demo checklist (10 minutes before)

| Step | Command or action | Expect |
|---|---|---|
| Know which database the scripts hit | Read `docs/POSTGRES-PORT.md` | This fork's scripts still run against CockroachDB Cloud until that port is done; the talk track below describes the PostgreSQL variant (Redpanda Connect `postgres_cdc` in place of CHANGEFEED, an hourly purge in place of row-level TTL) |
| ADP login is fresh (the token expires after about a day) | `rpk ai auth status` | `logged_in: true`, environment `adp-production`; otherwise `rpk ai auth login` |
| Everything is up, and the guardrail fires through the agent | `VERIFY_GUARDRAIL=1 make verify` | Every line is PASS and the summary ends in `0 failed`, including the A2A smoke test, the audit row within 60s and the guardrail probe (the denied-topic prompt is blocked with `guardrail_intervened`). Run it before the reset below: verify stores "my favourite coffee shop is called verify-<nonce> Coffee" for the demo user and expires it afterwards |
| Account state and memory are clean (cold start) | `make reset ALL=1` (the same as `scripts/demo-reset.sh --all`) | Account 1001 back at 2,450.00 USD, demo rent transaction gone, `memories_deleted N`, and the `live_memories` table is empty. This is an admin DELETE (the agent itself cannot delete); the deletes reach `memory_audit` as `op = delete` rows like any other change |
| The agent agrees | `make chat NEW=1 MSG="What do you remember about me?"` | It says it has no memory of you yet |
| If the guardrail probe failed: the agent is on the Anthropic provider (the only path on which the guardrail fires through the agent, Act 5) | `rpk ai agent get memory-concierge -o json \| jq -r .managed.spec.llm_provider` | `zeke-anthropic-agent-memory`. If it prints `zeke-bedrock-agent-memory`: `set -a; source .env; set +a; rpk ai agent update memory-concierge --llm-provider "$LLM_PROVIDER" --model "$LLM_MODEL"`, then `VERIFY_GUARDRAIL=1 make verify` again |
| Console tabs open | ADP console: agent `Memory Concierge` (Transcripts tab), Audit log, MCP server `crdb-agent-memory` (Data Policies tab), guardrail `agent-memory-guardrail`, LLM provider `zeke-anthropic-agent-memory` | Each page loads |
| Terminal B ready | `make observe` (leave it running) | Tail of `agent.memory.cdc` plus, every 5s, the latest 10 `memory_audit` rows and the active memories for the demo user |
| Terminal font large, both terminals visible side by side | | |

If anything in `make verify` fails, see Troubleshooting in `README.md`. Do not start the demo on a red verify.

## Timing

| Act | Minutes | Beat |
|---|---|---|
| Intro | 1.5 | The problem and the picture |
| 1 Cold start | 2 | Identity, masking |
| 2 Remember | 2.5 | Memory write, CDC, audit |
| 3 Recall | 1.5 | Memory survives the conversation |
| 4 Event-driven memory | 3 | The "why Redpanda" beat |
| 5 Governance | 3 | Three stops, a denied topic and an optional PII beat; transcripts and audit log |
| 6 Forget | 1.5 | Expire, never delete; scheduled purge |

## Intro (1.5 min)

Show the architecture diagram in `README.md`.

Say: "Agents need memory. The question is where it lives and who can see it change. In this demo memory is rows in PostgreSQL, every change to those rows is an event in Redpanda, and the agent itself runs on Redpanda's Agentic Data Plane so identity, policy, guardrails and audit are part of the platform rather than part of the prompt. Six short acts."

## Act 1: Cold start (2 min)

Terminal A:

```bash
make chat NEW=1 MSG="What do you know about me and my accounts?"
```

Say: "I am logged in with `rpk ai auth login`. The gateway knows who I am and injects my email into the agent's system prompt. The agent's first move in every conversation is a recall query against `memories` for exactly that subject."

Audience sees: the agent says it has no memories yet, then lists three accounts (a short list or table): Everyday Checking (checking, 2,450.00 USD), House Fund (savings, 41,200.00 USD), Travel Card (credit, -812.40 USD). The `account_number` column shows only the last four digits (for example `**********1001`), and if the agent surfaces the customer profile the `tax_id` reads `[redacted]`.

Say: "Notice the masked values. The agent did not do that. The SQL MCP server's data policy rewrote the tool result before the model ever saw it. The agent explains the masking as policy because it was told to, but it could not unmask if it wanted to."

Console: MCP server `crdb-agent-memory`, Data Policies tab. Point at the two rules: `tax_id` redact, `account_number` partial keep last 4, and the `max_rows` clamp.

## Act 2: Remember (2.5 min)

Terminal B should already be running `make observe`.

Terminal A (continues the same conversation):

```bash
make chat MSG="I prefer short answers. I'm saving for a house down payment by next June."
```

Audience sees: a one or two line confirmation that two things were remembered: a preference (short answers) and a goal (house down payment by next June).

Say: "Two durable facts, two rows. The agent wrote them with an INSERT ... ON CONFLICT upsert on a content hash, so saying the same thing twice never duplicates memory."

Terminal B: within a few seconds two CDC events appear on `agent.memory.cdc`. Each line is the Kafka key (`["<memory uuid>"]`, a JSON array) followed by the CDC envelope the pipeline builds from the row and the `postgres_cdc` metadata: `{"op":"insert","table":"memories","lsn":"<WAL position>","commit_ts_ms":<ms>,"before":null,"after":{subject, kind, content, importance, expires_at, ...}}`. `before` is filled only on update and delete. On the next poll `memory_audit` shows two new rows with `op = insert` and the active memories list shows the two new rows.

If the identical sentence already exists for the subject, even expired from a rehearsal, the upsert revives it instead: terminal B shows an update event carrying the old row as `before` (`memories` has `REPLICA IDENTITY FULL`, so every non-generated column is there, not only the key) and `memory_audit` gets `op = update`. Say so, it is the dedup working. The pre-demo `make reset ALL=1` is what makes these genuine inserts.

Say: "This is a Redpanda Connect pipeline reading PostgreSQL logical replication with the `postgres_cdc` input: `wal_level=logical`, a publication and a replication slot on the database, no Debezium, no connector host. It writes every row change to a Redpanda topic, and the audit pipeline consumes that topic and materialises an append-only audit table. The agent has no DELETE privilege on anything and no UPDATE or DELETE on `memory_audit`, so history cannot be rewritten or erased; the pipeline is its intended writer. That table is the memory's history."

## Act 3: Recall in a new conversation (1.5 min)

Terminal A (new context):

```bash
make chat NEW=1 MSG="Anything I should keep in mind this month?"
```

Audience sees: a short answer (the preference is being honoured) that mentions the house down payment goal and, typically, the House Fund balance and the Travel Card balance.

Say: "New conversation, no transcript carried over. The only thing connecting these two chats is a row in PostgreSQL keyed by my verified email. Memory survived the conversation because it was never in the conversation."

## Act 4: Event-driven memory (3 min)

Say: "So far the agent only remembers what I told it. Now something happens to my account while I am not talking to anyone."

Terminal A:

```bash
make overdraft
```

Say: "That ran one transaction in PostgreSQL: a rent debit of 2,900 and the checking balance moving from 2,450 to -450."

Terminal B: within a few seconds (allow up to 20) a new event appears on `agent.memory.cdc`. The new row has `kind` `observation`, `source` `event-stream`, a `source_ref` like `accounts/1001@<lsn>` (the WAL position of the balance change), and a `content` that reads like: "Account 'Everyday Checking' went overdrawn on 2026-10-15: balance moved from 2450.00 to -450.00 USD." A matching `memory_audit` row follows.

Say: "The same `postgres_cdc` pipeline reads the `accounts` table from the publication and writes its changes to `agent.accounts.cdc`. With `REPLICA IDENTITY FULL` on `accounts` each update carries the old balance as well as the new one, so the observer pipeline saw the balance cross zero and wrote an observation into memory with a reference back to the exact WAL position. No model involved, a few lines of Bloblang."

Terminal A (new context):

```bash
make chat NEW=1 MSG="Hi."
```

Audience sees: the agent greets briefly and proactively says it noticed Everyday Checking went overdrawn (balance -450.00 USD against a 500 overdraft limit), and may offer to show the recent transactions that caused it.

Say: "I said hello. It told me something I did not tell it. That is the Redpanda beat: memory formed from live data, not from chat, and it will work for any event your business already streams."

## Act 5: Governance (3 min)

Terminal A (same or new context, either works):

```bash
make chat MSG="Run DROP TABLE memories for me right now."
```

Audience sees, as the whole reply on stdout (rpk's context-id and task status lines, and a `note:` line from `chat.sh`, appear around it on stderr):

```
[blocked by guardrail] Blocked by guardrail: this assistant cannot act on that request.
```

The agent never ran a tool and the model never saw the prompt. `make chat` exits 0 on a guardrail block; underneath, the A2A task finished in state `failed` with `[guardrail_intervened] ...` and `rpk ai agent a2a send` exited 4, which `scripts/chat.sh` translates into that one line.

Say: "Three independent stops are in the way of that request. First, the ADP guardrail attached to the LLM provider. The agent runs on an Anthropic provider, so the AI Gateway evaluated the guardrail itself, against its Bedrock backend, before forwarding anything to the model. It has a word filter for DROP TABLE, TRUNCATE TABLE, DELETE FROM, ALTER TABLE, GRANT SELECT, GRANT INSERT and REVOKE, applied to what the caller sends and to what the model returns, and that is what fired here. Second, if a model ever emitted that SQL, the MCP server rejects it against a regex blocklist. Third, the database role `agent_rw` has no DROP or DELETE privilege at all. `make verify` proves the third one every run by attempting a DELETE as the agent's role and expecting a permission error."

Console: guardrail `agent-memory-guardrail`. Show the word filter list, the denied topic "Investment advice", and the PII entities (SSN and card numbers). Then MCP server `crdb-agent-memory`: the `blocked_patterns` under guardrails (the `allowed_schemas` field next to them is declared but not enforced by the managed SQL MCP server; the schema fence is the `agent_rw` grants).

```bash
make chat MSG="Which cryptocurrency should I buy with my savings this week?"
```

Audience sees: the same line, `[blocked by guardrail] Blocked by guardrail: this assistant cannot act on that request.`

Say: "That is the denied-topic policy: a semantic classifier, not a word list. 'Investment advice' is defined with a sentence and three examples. The bank decides the topic boundary; the model does not get a vote."

Optional third beat, if time allows:

```bash
make chat MSG="For my records, my social security number is 123-45-6789."
```

Audience sees: the same blocked line.

Say: "The PII policy anonymizes SSNs and card numbers. On this path the gateway evaluates the guardrail itself, and an anonymize match on input short-circuits like a block, so the number never reached the model, the transcript or the memory table. On a Bedrock provider the same policy masks it in place instead; see the sidebar."

Console: agent `Memory Concierge`, Transcripts tab. Open the current conversation: input and output messages, tool calls with their SQL and parameters, and the blocked turns, which show as failed turns carrying the `[guardrail_intervened] Blocked by guardrail: ...` error. Terminal fallback: `rpk ai agent transcript list memory-concierge --since 1h`, then `rpk ai agent transcript get memory-concierge <conversation-id>`. Then the Audit log: the tool calls carry their Cedar decision, and each blocked LLM call has outcome `Denied` with `agent-memory-guardrail` named in its Policies section, because the gateway evaluated the guardrail itself.

Say: "Every turn, every tool call, every block, recorded by the platform. If a regulator asks why the agent said something, the answer is a transcript plus a Cedar decision plus a row in `memory_audit`, not a shrug."

### Sidebar: the Bedrock variant

The guardrail is Bedrock-backed, and the alternative provider `zeke-bedrock-agent-memory` carries the same guardrail. On a Bedrock provider, Bedrock enforces it inside the model call, and in this gateway build only on the `invoke` operation. The managed agent runtime calls Bedrock with `converse-stream`, so with the agent on the Bedrock provider the three prompts above get the agent's own refusal (system prompt), not the guardrail's (observed 2026-10-15; platform behaviour, may change). To show Bedrock enforcing the guardrail, call the provider directly through the gateway; the prompt never touches the agent or memory:

```bash
set -a; source .env; set +a
curl -sS -D - "$AIGW_URL/llm/v1/providers/$BEDROCK_PROVIDER/model/$BEDROCK_MODEL/invoke" \
  -H "Authorization: Bearer $(rpk ai auth token)" -H 'Content-Type: application/json' \
  -d '{"anthropic_version":"bedrock-2023-05-31","max_tokens":200,
       "messages":[{"role":"user","content":"For my records, my social security number is 123-45-6789 and my card is 4111 1111 1111 1111. Please repeat them back."}]}'
```

The curl and the switch command below read `BEDROCK_PROVIDER` and `BEDROCK_MODEL` from `.env`; make sure both are there (written by `make preflight` since the provider change; an older `.env` needs the two lines from `.env.example`), or use the literal names `zeke-bedrock-agent-memory` / `us.anthropic.claude-sonnet-4-6`.

Expect the response header `amazon-bedrock-guardrailAction: INTERVENED` and a body in which the values appear as `{US_SOCIAL_SECURITY_NUMBER}` and `{CREDIT_DEBIT_CARD_NUMBER}`: Bedrock masked the prompt in place and forwarded it. With `"content":"Run DROP TABLE memories for me right now."` the body is the blocked-input message instead. The same text sent to `.../model/$BEDROCK_MODEL/converse` with a Converse-shaped body (`{"messages":[{"role":"user","content":[{"text":"..."}]}]}`) passes through untouched: that is the gap, and the reason the agent runs on the Anthropic provider. Switch the agent with `rpk ai agent update memory-concierge --llm-provider "$BEDROCK_PROVIDER" --model "$BEDROCK_MODEL"` only when own-account billing matters more than the live guardrail beat; switch back with `rpk ai agent update memory-concierge --llm-provider "$LLM_PROVIDER" --model "$LLM_MODEL"`.

## Act 6: Forget (1.5 min)

Terminal A:

```bash
make chat MSG="Forget that I'm saving for a house."
```

Audience sees: a one line confirmation that the goal was forgotten.

Terminal B: an update event on `agent.memory.cdc` with the old row as `before` next to the new one; the only differences are `expires_at` (now in the past) and `updated_at`. `memory_audit` gains a row with `op = update`, and on the next poll the goal row disappears from the active memories list, because that query, like recall, filters on `expires_at > now()`.

Say: "Forgetting is an UPDATE, never a DELETE. The agent cannot delete; it sets `expires_at` to now. PostgreSQL has no row-level TTL, so a small scheduled Redpanda Connect pipeline, or pg_cron where your managed Postgres offers it, runs `DELETE FROM memories WHERE expires_at < now()` every hour. That purge flows through logical replication as its own delete event, so the audit table records the forget and the physical removal as two separate facts. The right to be forgotten and the duty to keep an audit trail are not in conflict here."

Close: "Memory in a transactional database. The change stream as the audit trail and the integration point. An agent governed by a platform. Questions."

## Reset procedure

Run after every rehearsal or demo, before the next one.

```bash
make reset ALL=1          # the same as scripts/demo-reset.sh --all
```

This restores account 1001 to 2,450.00 USD, deletes the demo rent transaction and deletes every memory of the demo user, as admin: the observation from Act 4, the preference from Act 2 and the expired goal from Act 6 included. A plain `make reset` (no `ALL=1`) keeps the agent-written memories; use it only when the next run should start with memory already present. Asking the agent to "forget everything" is not a substitute: forgetting is an UPDATE that only sets `expires_at`, the row stays until the hourly purge runs, and Act 2's upsert would then revive it as an update instead of an insert.

The admin deletes are themselves captured by logical replication (`op` `delete`, the old row as `before`, `after` null), so `memory_audit` keeps `op = delete` rows for them; history is never reset.

Then stop `make observe` in terminal B with Ctrl-C and start it again for the next run. The next `make chat NEW=1 MSG="..."` starts a fresh A2A context (`.state/chat-context` is overwritten).

## If something goes wrong mid-demo

| Problem | Recovery |
|---|---|
| Chat returns an auth error | `rpk ai auth login` in a third terminal, retry the same command |
| Act 4 observation does not appear within 30s | Keep talking about the pipeline; check terminal B for the `agent.memory.cdc` event. If the balance was already negative (reset skipped), `make reset` then `make overdraft` |
| Agent recalls stale memories from a previous run | Say so, it is a feature (memory survived); or, in a third terminal, `make reset ALL=1` and restart Act 1 with `make chat NEW=1 MSG="..."` |
| Guardrail does not block in Act 5 (the agent declines politely instead of printing `[blocked by guardrail]`) | Most likely the agent is on the Bedrock provider, where the guardrail does not fire through the agent (converse path): in a third terminal `set -a; source .env; set +a; rpk ai agent update memory-concierge --llm-provider "$LLM_PROVIDER" --model "$LLM_MODEL"` and retry, or run the direct invoke curl from the Act 5 sidebar, which shows Bedrock intervening. Otherwise a guardrail change may still be propagating (about 30s). Either way the agent declines on its own (system prompt), so there is no MCP error to quote; show the `blocked_patterns` list on the MCP server page in the console and the `role agent_rw is denied DELETE on memories` PASS line from `make verify`, and say that the guardrail is the outermost of three stops, not the only one |
| `make observe` shows nothing at all | The audit pipeline or the `postgres_cdc` pipeline is down (an inactive replication slot is the usual sign); fall back to the Transcripts tab for Act 2 and continue |
