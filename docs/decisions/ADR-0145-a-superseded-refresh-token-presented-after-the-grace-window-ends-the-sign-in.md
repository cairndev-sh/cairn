# ADR-0145 — A superseded refresh token presented after the grace window ends the sign-in

**Status:** accepted (owner-approved 2026-10-22). Amends ADR-0144's consequences (the trade-off
that a takeover within the window stays undetected); ADR-0144 is otherwise unchanged.

## Context
ADR-0144 lets a rotated refresh token replayed within ten seconds of its rotation renew: the
child the rotation issued is revoked and a sibling takes its place. If the replay was a copy of
the token, the copy now holds the live sibling and the legitimate client holds the revoked
child. Under ADR-0144 that client's next refresh is simply refused: the person is signed out
and the copy keeps the sign-in, undetected.

That revoked child is distinguishable from every other revoked row: it was never used, and it
was revoked on its own (by a replay superseding it) while the rest of its family stayed live.
Rows revoked by sign-out, a password change, disabling or earlier reuse were revoked together
with their whole family.

## Decision
**D1 — A superseded child presented after the window ends the family.** In `validate`, a
presented row that is revoked, was never used (`used_at` empty) and was revoked at least
`REUSE_GRACE_SECONDS` (10) ago revokes its family. If that revoked anything (count > 0), it
logs a WARNING, "superseded refresh token presented", naming only the user (`sub`) and the first
8 characters of the family id. The request is refused (401, the cookie cleared) as before.

**D2 — Within the window it only refuses.** A superseded child presented less than ten seconds
after it was superseded is refused without revoking the family: that is a request that was
already in flight (a reload racing the replay), and ending the family there would kill the
fresh sibling the same browser is about to store.

**D3 — Rows revoked with their family end nothing more.** A token revoked by sign-out, a
password change, disabling or earlier reuse belongs to a family with nothing live left, so the
revocation counts 0 rows: no change, no log.

The store already records `revoked_at` as a timestamp; no schema change.

## Consequences
- ADR-0144's trade-off narrows: a copy replayed within the window still takes the sign-in over
  at first, but the takeover ends, for both holders, as soon as the displaced client presents
  its token ten seconds or more later (normally its next refresh), and it is logged as a WARNING.
- Two tabs that refreshed at once: if the browser kept the live sibling, nothing changes. If it
  kept the superseded child, the next refresh is refused as before, and now the family (whose
  live token nobody holds) is revoked and a WARNING logged. The person sees the same outcome;
  the log cannot tell this benign case from a takeover.
- A replay is still not detected while the displaced client stays silent; detection waits for
  its next refresh.
- A request carrying the superseded child that reaches the server ten seconds or more after the
  replay superseded it ends the family, even when it is only a slow in-flight request from the
  same browser that is about to store the sibling. That needs the request (or the 200 that
  answered the replay) delayed by more than ten seconds, which is pathological, and the person
  then signs in again.
- `revoked_at` is written by the replica that superseded the child and compared on the replica
  that receives it, under the same NTP assumption as ADR-0144. A `revoked_at` that appears to lie
  in the future (a lagging replica) gives a negative elapsed time, so the token is only refused;
  a replica whose clock runs ahead can end a family up to its lead earlier than ten seconds.
