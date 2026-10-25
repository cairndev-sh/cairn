# Security Policy

cairn is a governance control plane: it sits between an operator and the things
that cost money, leak data, or change production. A defect in that layer is a
security defect even when it looks like a bug. We would much rather hear about
it privately first.

## Reporting a vulnerability

**Use [GitHub Private Vulnerability Reporting](https://github.com/cairndev-sh/cairn/security/advisories/new)**
— Security → Report a vulnerability. It keeps the report private until a fix
ships, and gives us a place to talk it through with you.

If that is unavailable, email **security@cairndev.sh** with `SECURITY` in the
subject. Reports sent to that address go to the maintainers.

Please do **not** open a public issue for a suspected vulnerability. A public
issue is visible to everyone the moment it is filed, including anyone who would
use it.

**What to include:** what you did, what happened, what you expected, and the
version or commit. A minimal reproduction is worth more than a long report. If
you are unsure whether something counts, send it anyway — a false alarm costs
us ten minutes.

**What to expect:** acknowledgement within 3 working days, an assessment within
10, and a fix or a written explanation of why it is not one. We will credit you
in the advisory unless you ask us not to.

cairn is a young project maintained by a very small team. We do not run a bug
bounty and cannot pay for reports.

## Scope

The parts where a defect matters most:

- **The approval chokepoint** — any path that dispatches an operator without
  passing through `enforce_gate`, or that satisfies a gate without a real human
  decision. Bypassing a `requires_approval` operator is the highest-severity
  class of bug in this codebase.
- **Redaction before vendor egress** — any path that reaches an LLM provider
  without passing through the redaction chokepoint when one is bound.
- **The control-plane API** — authentication and authorisation on the trigger
  server, workspace and tenant isolation, privilege escalation between
  accounts.
- **Secret handling** — connection credentials, tokens in run state, anything
  that lands in logs, artifacts, audit records, or an error message.
- **The operator sandbox** — escaping mock/sandbox mode into real side effects.
- **Policy and budget enforcement** — evading a cost cap or a policy gate.
- **The audit trail** — forging, deleting, or breaking the hash chain.

## Out of scope

- Anything requiring credentials, physical access, or an already-compromised
  host.
- Vulnerabilities in third-party dependencies — report those upstream. Tell us
  anyway if cairn's usage makes an upstream issue materially worse.
- Deployments that disable their own guardrails. cairn lets an operator run
  ungoverned on purpose; choosing that is a configuration decision, not a
  vulnerability. A path that runs ungoverned *while reporting that it is
  governed* is very much in scope.
- Missing hardening with no demonstrated impact (headers, version disclosure,
  rate limits) unless you can show what it lets someone do.
- Denial of service by brute resource exhaustion.
- Self-hosted infrastructure belonging to the maintainers.

## Supported versions

cairn is pre-1.0 and moves fast. Fixes land on `main` and ship in the next
patch release of the current minor line on PyPI. They are not backported to
older lines; upgrade to the latest release.

| Version | Supported |
| ------- | --------- |
| `main`  | yes       |
| latest release (0.2.x) | yes: fixes ship in the next 0.2.x patch release |
| older releases | no: upgrade |

## Disclosure

We aim to fix, release, and publish an advisory before any public discussion.
If a report is still unfixed after 90 days, you are free to disclose it — we
would appreciate a heads-up, and if we are simply slow rather than silent, a
conversation about timing is usually easier than a deadline.
