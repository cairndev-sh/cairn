---
name: fan-out
version: 0.1.0
module: ask
description: One model call per item, in parallel, under a cap the second answer crosses.
budget:
  max_cost_usd: 0.00004
flow:
  type: state_machine
  source: behavior_tree
  root:
    type: map
    over: symptom
    as: item
    body:
      type: action
      ref: c0
  nodes:
    c0:
      type: prompt
      model: gpt-4o-mini
      capabilities: []
      prompt_section: c0
tools:
  required: []
---

# Fan out

## c0

Answer.
