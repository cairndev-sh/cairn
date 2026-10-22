# ADR-0144 — A refresh token replayed within ten seconds of its rotation renews instead of ending the sign-in

**Status:** accepted (owner-approved 2026-10-22). Amends ADR-0142 D4 (reuse of a rotated
refresh token revokes the family); ADR-0142 is otherwise unchanged.

## Context
ADR-0142 D4 treats any presentation of an already-rotated refresh token as theft and revokes
the whole family. Legitimate clients also present a rotated token: a refresh whose response
was lost (a dropped connection, a closed laptop, a proxy timeout) leaves the browser holding
the old cookie, and two tabs that refresh at the same moment send the same cookie twice, so
the loser of the compare-and-set looks like a thief. Each of these signs the person out.
Identity providers allow a short reuse interval for exactly this (Auth0's reuse interval,
Okta's grace period, Supabase's `refresh_token_reuse_interval`).

## Decision
**D1 — A ten-second window.** A rotated token presented again less than
`REUSE_GRACE_SECONDS` (10, a module constant in `cairn/core/refresh_tokens.py`, not a setting)
after its own rotation (`used_at`), while its family is not revoked and not expired, renews
instead of being refused.

**D2 — A replay renews as a sibling; a parent has at most one live child.** The child the
token was rotated into is revoked (the browser never stored it) and a new token in the same
family takes its place (`replaced_by` points at it); the cookie is set and a fresh access token
returned, exactly as for a normal refresh. A second replay of the same parent inside the window
does the same again, revoking the previous sibling. The child is revoked with a conditional
update (`… WHERE used_at IS NULL AND revoked_at IS NULL`), and the parent is pointed at the
sibling with a compare-and-set on `replaced_by`, which is the decision: of two concurrent
replays exactly one installs its sibling, and the other re-reads the parent and revokes that
sibling in turn. A child another replay already revoked is dead either way, so a replay that
finds it so goes straight to the compare-and-set instead of waiting for the other replay to
finish. A replay gives up (and the family is revoked) only after five passes in which another
replay installed its sibling first; a slow or stalled replay costs no attempts.

**D3 — Never two live chains.** If the child the replayed token was rotated into has itself
already been rotated, renewing beside it would leave two live chains, so that replay is reuse:
the family is revoked, window or not.

**D4 — The race path follows the same rule.** Of two concurrent refreshes with one token, the
loser of the compare-and-set is a replay within the window and gets the sibling treatment
instead of revoking the family.

**D5 — Outside the window nothing changes.** A replay ten seconds or more after the rotation
revokes the family as ADR-0142 D4 says. A replay of a token whose family was revoked (sign-out,
password change, disabling, earlier reuse) is refused.

**D6 — Logging.** A replay within the window logs at INFO; reuse outside it at WARNING, as
before. Both name only the user (`sub`) and the first 8 characters of the family id.

## Consequences
- A lost refresh response, or two tabs refreshing at once, no longer signs the person out.
- **Trade-off:** a replay within ten seconds of a rotation is not detected as theft. Someone who
  copies a refresh token and uses it within ten seconds of the legitimate client's refresh takes
  over the sign-in, and the legitimate client's token is the one revoked; that client's next
  refresh is then refused (its child was revoked), so the person is signed out and the takeover
  stays undetected rather than ending the family. Outside the window detection is as before.
- The two-tab case is better but not perfect: whichever response the browser stores last wins
  the shared cookie, and if that is the revoked sibling the next refresh fails.
- The window is measured between clocks: `used_at` is written by whichever replica rotated the
  token and compared on whichever replica receives the replay. The replicas are assumed to keep
  time with NTP. A rotation that appears up to 2 s in the future (a replica slightly ahead)
  still counts as just now; one more than 2 s in the future (a lagging replica, an NTP step) is
  not trusted, and the replay is treated as outside the window, so reuse rules apply.
- Store gains two conditional updates on every backend (`revoke_unused`, `set_replaced_by`); no
  schema change.
