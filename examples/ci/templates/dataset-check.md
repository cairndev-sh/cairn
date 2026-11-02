---
name: dataset-check
version: 0.1.0
module: ci
description: |
  Profile a dataset file against the roles it must contain and report a
  verdict for `cairn run --check`. Read-only, no model, no API key.

flow:
  type: state_machine
  source: behavior_tree
  root:
    # A selector stops at the first child that succeeds, so the second child runs
    # only when the file can't be read.
    type: selector
    children:
      # data/profile ships with cairndev. It reports `pass`, `degrade` or `block`.
      - type: operation
        operator: data/profile
        id: profile
        input:
          source: $input.path
          # What the data must contain, as roles rather than column names.
          # Edit these for your dataset.
          expectation:
            kind: rows
            roles:
              - {name: text, required: true, aliases: [body, content]}
              - {name: label, required: true, aliases: [category]}
              - {name: id, required: false}
        # `cairn run --check` reads these three from the top of the run's state, so they
        # are `$state.verdict`, not `$state.findings.verdict`.
        output:
          verdict: $state.verdict
          missing_required: $state.blocking
          degraded: $state.warnings
      # A missing or malformed file makes data/profile fail, which leaves no verdict.
      # Profiling one empty row against a role it can't have blocks instead, and
      # the role's name is the reason the gate prints.
      - type: operation
        operator: data/profile
        id: unreadable
        input:
          rows: [{}]
          expectation:
            kind: rows
            roles:
              - {name: dataset file could not be read, required: true}
        output:
          verdict: $state.verdict
          missing_required: $state.blocking
  nodes: {}

tools:
  required: []
---

# Dataset check

Operation nodes and no model: the verdict comes from the data, so the same file
always gets the same answer.
