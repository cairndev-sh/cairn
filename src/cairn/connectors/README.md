# MCP connector presets

Known-good specs for official MCP servers, so a deployment doesn't have to
reverse-engineer each one's package, transport, and required env. Reference +
convenience — **not** a new config layer.

Shipped: `github`, `slack`, `notion`.

## Use

```bash
cairn admin connect presets             # list them: name, transport, required env, description
cairn admin connect add --preset github # → a kind=mcp connection named "github"
cairn admin connect test github         # resolve the token ref + probe
```

`--preset` goes through the same path as `cairn admin connect add mcp` (the
connection store). The preset's `[server]` fills the connection config and
its required `[env]` vars become secret *references* (`GITHUB_PERSONAL_ACCESS_TOKEN
→ GITHUB_PERSONAL_ACCESS_TOKEN`), resolved when the connection is used — export
the var, or point the ref elsewhere with `--secret KEY=REF`. `--name`, `--set
key=value` and `--description` override the preset's values. Secret values are
never stored or printed.

Or copy a preset's `[server]` block into a profile's
`[profile.mcp_servers.<name>]`. `cairn/services/mcp/presets.py` is the single
reader of these files.

## How it fits the safety model

- A **pack** declares the tools it needs: `[requires.mcp]\ntools = ["github:list_issues", …]`.
- A **profile** sets `tool_allow_list_strict = true` on the server → the MCP
  client denies any tool outside the pack-declared surface (`McpToolNotAllowed`).
- `cairn setup --check` checks declared servers are reachable.

## Adding a preset

Drop a `<name>.toml` here with `[connector]`, `[server]` (transport + command),
`[env]` (required/optional vars), `[scopes]` (least-privilege note), `[tools]`
(representative surface). It's picked up automatically by `cairn admin connect presets` / `--preset`.
