---
title: Glossary
description: The words the cairn docs use, in alphabetical order, each with a short definition and a link to the page that explains it.
---

The words below are used the same way across these docs. For how they fit
together, see [How cairn fits together](/concepts/architecture/).

#### Agentic flow
A [flow](#flow) written as a markdown file whose front matter holds a prompt or
a state machine of nodes. A [`template:` leg](#template) runs it on the
[runtime](#runtime). See [Flows](/concepts/flows/).

#### Approval
A person's recorded decision on a [gate](#gate): who decided, when, and any
rationale. A pipeline waiting for one is paused, and the wait is stored. See
[Gates and approvals](/concepts/gates/).

#### Artifact
An output that an [engine](#engine) reports and cairn registers. Its id is its
content hash and it does not change once written. See
[Runs and the record](/concepts/runs/#artifacts).

#### Audit log
An append-only file of security-relevant events in which each row carries a
hash of the row before it, so a changed row breaks the chain. It is on only when
the [profile](#profile) binds `audit.sink`, and a template's prompt and agent
nodes write no row yet. See
[The audit trail](/concepts/runs/#the-audit-trail).

#### Budget
The spending cap a pipeline declares in `governance.budget`. A deployment can
set a default and a ceiling in its [profile](#profile). See
[Cost and budgets](/concepts/cost/).

#### Capability
A name for a kind of work, such as `storage.put` or `secrets.store`. A step asks
for a capability, and the [profile](#profile) binds it to the [operator](#operator)
that provides it. See [Capabilities and environments](/concepts/capabilities/).

#### Data flow
A [flow](#flow) of `kind: data`: an ordered list of [stages](#stage) over a
dataset, in `flows/<name>.yaml`. A `flow:` leg runs it on an [engine](#engine).
See [Flows](/concepts/flows/).

#### Engine
Short for dataflow engine: what runs the stages of a data or ML flow for a
`flow:` leg. The built-in one is the default; Metaflow and SkyPilot come as
extras. The [profile](#profile) picks it. The docs also say "the engine" for the
cairn server itself. See
[Capabilities and environments](/concepts/capabilities/#environments).

#### Environment
The deployment context a pipeline runs in, set by a [profile](#profile) and
chosen at launch (`--environment sandbox`). See
[Capabilities and environments](/concepts/capabilities/#environments).

#### Flow
A component that a pipeline leg runs, such as checking a file or fine-tuning an
adapter. A flow comes in three kinds, [data](#data-flow), [ML](#ml-flow) and
[agentic](#agentic-flow), and never runs on its own: only a [pipeline](#pipeline)
does. See [Flows](/concepts/flows/).

#### Gate
A [leg](#leg) that does no work and decides whether the legs after it may run.
It is answered by data (a predicate over earlier results) or by a person. A
policy can add a gate to a pipeline, and an operator can ask for approval on
each call. A gate has no [run](#run). See
[Gates and approvals](/concepts/gates/).

#### Leg
One entry in a pipeline's `steps:` list, with an `id` and one kind: `flow:`,
`template:`, `task:`, `gate:` or `loop:`. A leg that does work (flow, template
or task) is its own [run](#run). See
[Pipelines, legs and steps](/concepts/pipelines/#legs).

#### ML flow
A [flow](#flow) of `kind: ml`: the same shape as a data flow, for training and
evaluation. See [Flows](/concepts/flows/).

#### Operator
The code that does one kind of work with one tool or provider. It declares the
[capabilities](#capability) it provides and its [risk](#risk), and ships as its
own Python package. See
[Capabilities and environments](/concepts/capabilities/#capabilities-and-operators)
and the [operators reference](/reference/operators/).

#### Pack
A directory, or an installed package, with a `pack.toml`. A domain pack holds
pipelines, flows and policies; an operator pack holds operators. cairn sees a
pack's contents only once the pack is registered or installed. See
[Write a pipeline](/guides/write-a-pipeline/).

#### Pipeline
The unit you launch: a YAML file that lists [legs](#leg), their order and the
[gates](#gate) between them. Only a declared pipeline runs. See
[Pipelines, legs and steps](/concepts/pipelines/).

#### Pipeline record
The durable record of one launch (`pl_…`): the plan as resolved, the inputs,
each leg's status and run id, and each gate's verdict and approver. See
[Runs and the record](/concepts/runs/#the-pipeline-record).

#### Policy
A YAML rule that matches pipeline legs or operator calls and can require a
person's approval or refuse the launch. The pipeline author cannot remove it. See
[Gates an environment adds](/concepts/gates/#gates-an-environment-adds).

#### Profile
A TOML file that binds [capabilities](#capability) to [operators](#operator) and
picks the backends a run uses, such as the dataflow engine. The docs call the
chosen profile the [environment](#environment). A deployment names its base
profile with `CAIRN_PROFILE`. See
[Capabilities and environments](/concepts/capabilities/#environments).

#### Reservation
The hold a flow leg takes on its [series](#series) cap before its engine starts:
the pipeline's `estimated_usd`. A leg that does not fit fails before it spends.
It settles when the leg finishes and is released when the leg fails or is
cancelled, except for spend already made. See
[Cost and budgets](/concepts/cost/#budgets-on-the-pipeline).

#### Risk
A class an [operator](#operator) declares for itself: `read_only`,
`external_write`, `paid_compute` or `production_mutation`. Policies and
environments can match on it. See the
[operators reference](/reference/operators/#reading-the-table).

#### Run
One tracked execution with its own status, inputs, results and cost. Every flow,
template or task [leg](#leg) is its own run, admitted through the same checks as
any other. A pipeline is not a run: it has a [pipeline record](#pipeline-record)
that points to its runs. See [Runs and the record](/concepts/runs/#leg-runs).

#### Runtime
What executes an agentic flow's graph, and how durable that is: in-process (the
default), DBOS or Temporal, chosen with `CAIRN_RUNTIME`. Data and ML flows run on
an [engine](#engine) instead. See
[Choose the backends](/install/#choose-the-backends).

#### Series
A named group of runs that share one spending cap, set with `governance.series`.
See [Cost and budgets](/concepts/cost/#budgets-on-the-pipeline).

#### Stage
One unit of a [data or ML flow](#flow), such as `train.lora`. Installed packages
register stages, and a flow lists them in order. See
[Write a flow](/guides/write-a-flow/#your-own-stage).

#### Step
Used two ways. In a pipeline file the list of [legs](#leg) is called `steps:`,
and `$steps.<leg>.<field>` reads a leg's output. Inside a flow, a step is a
[stage](#stage) or a node. See [Pipelines, legs and steps](/concepts/pipelines/).

#### Task
A [leg](#leg) that calls one [operator](#operator) directly, written `task:`. It
is its own [run](#run). See [Pipelines, legs and steps](/concepts/pipelines/#legs).

#### Template
The leg kind, `template:`, that runs an [agentic flow](#agentic-flow). The word
also survives in some field names. See [Flows](/concepts/flows/).

#### Trigger
A declaration that starts a pipeline without a person: a schedule, a webhook, a
watch on a dataset, or an event another pipeline emits. A trigger fires only when
it says `enabled: true`, and it goes through the same launch door as a person.
See [Triggers belong to pipelines](/concepts/pipelines/#triggers-belong-to-pipelines).
