---
title: How cairn compares
description: "What cairn does next to Airflow, Prefect, Metaflow, Kubeflow, MLflow, promptfoo and LangGraph, what it hands to each, when to use both, and what cairn is not."
---

cairn decides when each step of the work behind a model may run, holds the
spend to a budget, pauses for data checks and people, and records what happened.
It leaves the work itself to other tools, so most of the tools on this page do a
different job from cairn, and some of them do parts of its job too.

cairn has code for three of them: LangGraph, Metaflow and MLflow. Airflow,
Prefect, Kubeflow and promptfoo have no integration in cairn. What this page
suggests for those four is ordinary commands and HTTP calls, or an operator you
write yourself, and cairn doesn't test any of it.

| Tool | What cairn does | What it hands to the tool | Use both when |
|---|---|---|---|
| [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html) | Runs a pipeline of AI work, with gates, a budget and a record for each leg | Nothing. There is no adapter. | Airflow [schedules your wider workflows](https://airflow.apache.org/docs/apache-airflow/stable/index.html), and one of its tasks starts a cairn pipeline |
| [Prefect](https://www.prefect.io/docs/v3/get-started) | The same | Nothing. There is no adapter. | Your workflows are [Python functions in Prefect](https://www.prefect.io/docs/v3/concepts/flows), and one of them starts a cairn pipeline |
| [Metaflow](https://docs.metaflow.org/) | Decides whether a flow leg may start, reserves its estimated cost, and records it | The leg's stages, as a Metaflow run on local, Kubernetes or AWS Batch compute (`cairndev[metaflow]`) | You want [Metaflow's compute](https://docs.metaflow.org/scaling/introduction) with approvals and a budget around it |
| [Kubeflow](https://www.kubeflow.org/docs/started/introduction/) | Gates and records the steps around the job | Nothing, unless you write an operator that submits the job | [Kubeflow Trainer](https://www.kubeflow.org/docs/components/trainer/overview/) trains on your cluster, and a person or a number decides what happens next |
| [MLflow](https://mlflow.org/docs/latest/ml/tracking/) | Keeps the decisions: the plan, the gates, who approved, the cost | Experiment tracking: on a pipeline that declares the input, `-i report_to=mlflow` asks the trainer to log there | You want [metric history and run comparison](https://mlflow.org/docs/latest/ml/tracking/) in MLflow and the decisions in cairn |
| [promptfoo](https://www.promptfoo.dev/docs/intro/) | Stops a pipeline on any number an earlier leg produced | Nothing. There is no integration. | [promptfoo tests your prompts in CI](https://www.promptfoo.dev/docs/integrations/ci-cd/), and cairn runs the pipelines that ship models |
| [LangGraph](https://docs.langchain.com/oss/python/langgraph/overview) | Compiles a `state_machine` flow, and governs it as one leg of a pipeline | Running that graph and pausing it | Always, for agentic flows: it is a required dependency |

## Airflow

Airflow is [a platform for developing, scheduling and monitoring
workflows](https://airflow.apache.org/docs/apache-airflow/stable/index.html),
built for [batch workflows with a clear start and end, and able to connect to
virtually any technology](https://airflow.apache.org/docs/apache-airflow/stable/index.html),
and its docs name machine learning, model training and agentic workloads among them.
It also has [approval and other human-in-the-loop
operators](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/hitl.html),
added in version 3.1. If your job is scheduling batch work across many systems,
Airflow is the better choice, and you may not need cairn.

To use both, an Airflow task starts a cairn pipeline: a
[`BashOperator`](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/bash.html)
runs `cairn run`, or an
[`HttpOperator`](https://airflow.apache.org/docs/apache-airflow-providers-http/stable/operators.html)
calls the pipeline's [webhook trigger](/reference/yaml/#triggers). Airflow [fails
a `BashOperator` task on a non-zero exit
code](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/bash.html),
and `cairn run` [exits
2](/reference/cli/#pipelines) when a pipeline is waiting on a person and there is
no terminal to ask on. So start a pipeline that has a person gate with
`cairn run --detach`, against a running cairn server: without one, `cairn run`
starts a temporary engine and ignores `--detach`. Airflow then sees the start, not
the result. The worker needs a login to that server, and
[logins expire and aren't renewed](/guides/fine-tune-a-model/#known-limits). With sign-in
on, a webhook trigger takes a bearer token or, for a signed preset, a signature.

## Prefect

Prefect is [an open-source orchestration engine that turns Python functions into
data pipelines](https://www.prefect.io/docs/v3/get-started). [Almost any Python
function](https://www.prefect.io/docs/v3/concepts/flows) can become a flow, and
flows [start from schedules, external events or its
API](https://www.prefect.io/docs/v3/get-started). A flow can also [pause
until a person answers in Prefect's
UI](https://www.prefect.io/docs/v3/advanced/interactive). If your team wants its
workflows to stay Python functions, Prefect is the better fit: a cairn pipeline
is a declared file of legs and gates, usually YAML.

To use both, a Prefect flow starts a cairn pipeline with `cairn run --detach` or
through a [webhook trigger](/reference/yaml/#triggers), as above.

## Metaflow

Metaflow is for [building and managing real-life data science, AI and ML
projects](https://docs.metaflow.org/). You [scale a flow with
`@resources`](https://docs.metaflow.org/scaling/introduction) onto Kubernetes or
AWS Batch, [inspect past runs with its client
API](https://docs.metaflow.org/metaflow/client), and [schedule flows on Argo
Workflows, AWS Step Functions, Airflow or
Kubeflow](https://docs.metaflow.org/production/introduction). If your team wants
to write its flows in Python and doesn't need pipeline-level gates and budgets,
use Metaflow alone.

To use both, install `cairndev[metaflow]` and [set `dataflow_engine` or
`ml_engine` to `metaflow`](/concepts/capabilities/). cairn decides whether a flow
leg may start and reserves its estimated cost, then Metaflow does the run. cairn
starts that run itself. It doesn't deploy to Metaflow's production schedulers.

`cairn pack export --to metaflow` writes a flow out as a plain Metaflow file, and
refuses a flow with a gate inside it.
Gates, budgets, admission checks, the audit chain and lineage don't travel with
it, so running that file runs the work and asks no one.

## Kubeflow

Kubeflow is [a set of modular projects that form a Kubernetes-native stack for
data and AI workloads](https://www.kubeflow.org/docs/started/introduction/).
[Kubeflow Pipelines](https://www.kubeflow.org/docs/components/pipelines/overview/)
builds and deploys portable ML workflows in containers on Kubernetes, and
[Kubeflow Trainer](https://www.kubeflow.org/docs/components/trainer/overview/)
does distributed LLM fine-tuning across frameworks such as PyTorch, HuggingFace
and JAX. If your team runs on Kubernetes and needs distributed training, Kubeflow
is the better choice. The open-source fine-tune path in cairn supports [one GPU
box per leg](/guides/fine-tune-a-model/#known-limits).

To use both, write an [operator](/reference/operators/) that submits the job to
Kubeflow (`cairn pack new operator` scaffolds one), then put gates after that leg.
cairn's Helm chart deploys cairn on Kubernetes, and that has nothing to do with
Kubeflow.

## MLflow

MLflow Tracking [logs parameters, code versions, metrics and output files, and
its UI compares runs across
experiments](https://mlflow.org/docs/latest/ml/tracking/). The [Model
Registry](https://mlflow.org/docs/latest/ml/model-registry/) is a central store
for model versions and their lineage, and MLflow also covers [tracing, evaluation
and prompt management for GenAI](https://mlflow.org/docs/latest/genai/). Use
MLflow for metric history and for comparing runs. cairn is not built for either.

To use both, run a pipeline that declares the input, such as those in
`cairn-pack-finetune`, with [`-i report_to=mlflow`](/guides/fine-tune-a-model/#what-to-change-for-your-own-run)
and `MLFLOW_TRACKING_URI` set on the control box. The input goes to the trainer,
cairn forwards the tracking settings to the box, and the trainer logs to MLflow.
cairn doesn't log to MLflow itself, and its gates read leg outputs, not MLflow. The
Helm chart can also start an MLflow service for you (`profiles.mlflow`).

## promptfoo

promptfoo is [an open-source CLI and library for evaluating and red-teaming LLM
apps](https://www.promptfoo.dev/docs/intro/). It [compares prompts and models side
by side](https://www.promptfoo.dev/docs/intro/), [probes for failure modes with
adversarial plugins](https://www.promptfoo.dev/docs/red-team/), and [can fail a CI
build below a quality threshold](https://www.promptfoo.dev/docs/integrations/ci-cd/). For testing
prompts, use promptfoo. The built-in operators don't cover it: `train/eval-loss`
reads a held-out loss, and other evaluation operators are in
[cairn-extensions](https://github.com/cairndev-sh/cairn-extensions) and cairn-platform.

The simple way to use both is to keep them apart: promptfoo in CI on your prompts,
cairn on the pipelines that ship models. To put promptfoo inside a pipeline, write
an operator that runs `promptfoo eval`, [writes the results to a
file](https://www.promptfoo.dev/docs/usage/command-line/) and returns a pass rate.
A [data gate](/concepts/gates/#a-gate-answered-by-data) can then stop the pipeline
on that number.

## LangGraph

LangGraph is [a low-level orchestration framework and runtime for
agents](https://docs.langchain.com/oss/python/langgraph/overview), with durable
execution, streaming, human oversight and memory. cairn uses it. `langgraph` is a
required dependency, and a `state_machine` flow compiles to a LangGraph
`StateGraph`. An approval inside that flow pauses with [LangGraph's
`interrupt()`](https://docs.langchain.com/oss/python/langgraph/interrupts) and
resumes from a [checkpoint](https://docs.langchain.com/oss/python/langgraph/persistence).

The graph runs in cairn's process by default, or under DBOS or Temporal for durable
execution ([runtime settings](/install/)). No graph holds a data or ML leg: those run
on a dataflow engine, though the `langgraph` module is still loaded in the control plane.

If you are building an agent as an application, and want direct control of the
graph, streaming and long-term memory, use LangGraph directly. Use cairn when the
agent is one step of a larger piece of work. You write a cairn flow as markdown
and YAML, with the control flow declared and no tool loop, and cairn doesn't take
an existing LangGraph graph as a flow.

## What cairn is not

- **Not a general-purpose scheduler.** A pipeline can start on [a cron schedule, a
  webhook, a watch or another pipeline's event](/reference/yaml/#triggers). Those
  triggers exist to start cairn's own pipelines, and cron schedules fire inside the
  cairn server. cairn isn't built to run the rest of your batch work.
- **Not an experiment tracker or a model registry.** The [record](/concepts/runs/)
  holds each run's status, outputs, cost and artifacts, and `artifact/promote` moves
  a governed tag. The open-source package has no web console (`cairn-admin` is an optional
  read-only viewer) and no command for comparing runs. The web console is part of cairn-platform.
- **Not an evaluation framework.** A gate can stop on a number, but something else
  has to produce that number.
- **Not a data or training engine.** The built-in `train/lora` operator fine-tunes a
  small model on CPU. Anything larger runs on an engine or operator pack you install.
- **Not an agent framework.** Agentic flows run on LangGraph, and the author declares
  their control flow.
- **Not finished.** cairn is at version 0.3 and marked beta. Commands still change
  between minor versions: the [CLI reference](/reference/cli/#what---help-shows) lists
  the names that changed. Some combinations are refused for now: under the Temporal
  runtime, a pipeline with a `task:` leg or a `finalizer:` step [won't
  launch](/install/).

## Next

- [What is cairn](/): the problem, and the five things cairn does.
- [Gates and approvals](/concepts/gates/): how a pipeline stops.
- [Cost and budgets](/concepts/cost/): how spend is capped.
- [Capabilities and environments](/concepts/capabilities/): how you choose the engines.
