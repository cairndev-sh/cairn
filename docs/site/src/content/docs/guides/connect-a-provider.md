---
title: Connect a provider
description: Point cairn at a model endpoint, register connections to the systems your pipelines reach, test them, and keep the secret values out of the record.
---

The sandbox needs nothing connected, and neither does the example's data
pipeline. To run pipelines that call a model or reach another system, give the
engine what it needs: a model endpoint, connections, and the secrets behind
them.

## A model endpoint

Model calls go to an OpenAI-compatible endpoint. Set one of these in the
**engine's** environment (for `cairn dev`, the shell you start it from; for the
Docker Compose stack, `deploy/light/.env`):

| Setting | Use |
|---|---|
| `OPENAI_API_KEY` | OpenAI itself |
| `OPENAI_BASE_URL` | any other OpenAI-compatible server, such as vLLM or Ollama |
| `LLM_GATEWAY=proxy`, `LLM_GATEWAY_URL`, `LLM_GATEWAY_API_KEY` | a LiteLLM gateway in front of many providers; the Compose stack's `llm-gateway` profile runs one |

Every model call is built in one place, which redacts the prompt (when the
profile binds `redact.text`) and records the call's cost against the run.

## Connections

A **connection** is a named, typed reference to an external system: a model
API, a compute target, an SSH host, an MCP server. It never holds a secret
value. For the deployment's own connections (the `default` account, which is
every connection when sign-in is off), a secret is an environment variable
**name** the engine reads, or an entry in your secret store.

With sign-in on, a connection belongs to the account that created it. A
connection of any other account resolves only the secrets stored for that
account (`cairn://secrets#<account>/<conn>-<KEY>`); an environment-variable
name, `${env:NAME}` and the deployment's secret store resolve to nothing for
it.

`cairn connect` manages connections in the engine's store, so run it with the
same `CAIRN_STORAGE` as the engine. When an engine answers at the endpoint `cairn
remote` uses, `connect add` and `connect rm` compare stores first and refuse with exit 3
if they differ (`--force` writes anyway):

```bash
cairn connect providers            # what you can add, and each one's secret keys
cairn connect add openai           # a connection that reads OPENAI_API_KEY
cairn connect add openai --name openai-eu --secret API_KEY=OPENAI_EU_KEY
cairn connect ls                   # names, kinds and secret keys, never values
cairn connect test openai          # authenticate now and store the result
cairn connect rm openai-eu         # removes the reference, never the secret
```

`cairn connect` manages the deployment's own connections (the `default`
account). A team account's connections are created by that account; the
console's integrations page and its API, which do this, are part of
cairn-platform.

`cairn connect add` saves the connection, then runs its health check once.
`cairn connect test` exits `0` when the provider answered and `1` when the
secret is missing or the check failed. `cairn connect presets` lists the
shipped MCP connection presets (GitHub, Slack, Notion), added with
`cairn connect add --preset <name>`.

### Example: an SSH host

The example's `nightly-ops` pipeline runs a report on a machine you own, over
SSH, through a connection named `hello-ssh`:

```bash
export HELLO_SSH_KEY="$(cat ~/.ssh/id_ed25519)"    # in the ENGINE's environment
CAIRN_STORAGE=sqlite:///$HOME/.cairn/dev.db \
cairn connect add ssh --name hello-ssh --set host=<box> --set user=<user> \
  --set known_hosts=$HOME/.ssh/known_hosts --secret PRIVATE_KEY=HELLO_SSH_KEY
```

The connection records the variable name `HELLO_SSH_KEY`; the key itself
never enters the record. `remote/exec` needs the `ssh` extra.

## Using a connection

Pipelines and flows refer to a connection **by name**, and its credential is
resolved only when the leg runs. The run record carries only the name.

A run resolves connection names in its own account: the account that owns the
run's workspace. Connection names are unique across the whole deployment, so a
team account creates its own connection under its own name (the console
suggests `<account>-<provider>`), and its pipelines and flows reference that
name. A team account's run does not see the deployment's connections. A
connection bound in the deployment's profile (`ml_target`, `dataflow_target`,
the backends) belongs to the deployment, and a team account's flow legs cannot
use it: they need a target connection of the account's own, named in the
pipeline or flow.

With sign-in on, a lookup made outside any workspace (a script, a background
job) raises `WorkspaceScopeRequired`. Enter a workspace first:

```python
from cairn.core.workspace import workspace_scope

with workspace_scope("<workspace id>"):
    ...
```

## Secrets in a secret store

Operators read secrets through the `secrets.store` capability, so where they
live is the environment's choice:

| Operator | Reads from |
|---|---|
| `secrets/env` | the engine's environment variables |
| `secrets/infisical` | an Infisical project (the `infisical` extra); set `INFISICAL_CLIENT_ID`, `INFISICAL_CLIENT_SECRET`, `INFISICAL_PROJECT_ID`, and `INFISICAL_HOST` for a self-hosted server |

The example ships both bindings: `profiles/hello.toml` uses `secrets/env`, and
`profiles/hello-infisical.toml` uses Infisical with nothing else changed. The
Docker Compose stack's `infisical` profile runs an Infisical server for you.

The bound secret store serves the deployment's runs. For a team account's run,
an operator's declared `secrets:` come from that account's own connections and
arrive in `ctx.secrets`, and the deployment's secret store is not read
(ADR-0143). A team account's run does not receive its account's credentials in
the process environment; read them from `ctx.secrets`.

The console's integrations page, and the operators that train or serve on GPU
providers, are part of cairn-platform.

## Next

- [Capabilities and environments](/concepts/capabilities/): how operators pick up a provider.
- [Run your first pipeline](/guides/first-pipeline/): launch, follow and read a pipeline.
- [Set up your team](/guides/team/): sign-in and who decides gates.
