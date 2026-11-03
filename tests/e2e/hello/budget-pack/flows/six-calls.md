---
name: six-calls
version: 0.1.0
module: ask
description: Six model calls under a cap that three of them cross.
budget:
  max_cost_usd: 0.00007
flow:
  type: state_machine
  source: behavior_tree
  root:
    type: sequence
    children:
      - type: action
        ref: c0
      - type: action
        ref: c1
      - type: action
        ref: c2
      - type: action
        ref: c3
      - type: action
        ref: c4
      - type: action
        ref: c5
  nodes:
    c0:
      type: prompt
      model: gpt-4o-mini
      capabilities: []
      prompt_section: c0
    c1:
      type: prompt
      model: gpt-4o-mini
      capabilities: []
      prompt_section: c1
    c2:
      type: prompt
      model: gpt-4o-mini
      capabilities: []
      prompt_section: c2
    c3:
      type: prompt
      model: gpt-4o-mini
      capabilities: []
      prompt_section: c3
    c4:
      type: prompt
      model: gpt-4o-mini
      capabilities: []
      prompt_section: c4
    c5:
      type: prompt
      model: gpt-4o-mini
      capabilities: []
      prompt_section: c5
tools:
  required: []
---

# Six calls

## c0

Answer.

## c1

Answer.

## c2

Answer.

## c3

Answer.

## c4

Answer.

## c5

Answer.
