---
name: one-call
version: 0.1.0
module: ask
description: One model call, to show whether a leg after the cap runs.
flow:
  type: state_machine
  source: behavior_tree
  root:
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

# One call

## c0

Answer.
