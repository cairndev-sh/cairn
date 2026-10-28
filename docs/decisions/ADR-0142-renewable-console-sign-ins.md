# ADR-0142 — Renewable console sign-ins: a rotating refresh cookie and server-side sign-out

**Status:** accepted (owner-approved 2026-10-22, "option 1"; review-round owner decisions the
same day are marked below; target release 0.1.5). Builds on
ADR-0054 (accounts and the `sub` / `account_id` claims), ADR-0094 (sign-up) and ADR-0119
(Google/GitHub sign-in hands the console the same access token as `POST /token`).

## Context
A console sign-in is a 30-minute access token (`CAIRN_ACCESS_TOKEN_EXPIRE_MINUTES`) and
nothing else, so a person is sent back to the login page every half hour, and signing out
only forgets the token in the browser: nothing on the server ends a sign-in. Lengthening the
access token would make every leaked token live longer, and a JWT cannot be revoked short of
rotating `CAIRN_JWT_SECRET`.

## Decision
**D1 — Every console sign-in also sets a refresh cookie.** `POST /token`, the Google/GitHub
landing (`GET /auth/oauth/{provider}/callback`) and `POST /auth/signup` keep returning the
same access token in the same place, and also set `cairn_refresh`: an opaque random token
(`secrets.token_urlsafe(32)`), `HttpOnly`, `SameSite=Strict`, `Path=/auth`, and `Secure` when
the deployment's public base is https (`CAIRN_PUBLIC_URL`, else the URL the request arrived
on), the rule the sign-in state cookie already follows. The server stores only the token's
SHA-256. No cookie with auth off.

**D2 — Remember.** `POST /token` takes an optional form field `remember` (default true, so
existing clients are unchanged). Remembered: the cookie's `Max-Age` and the server expiry are
`CAIRN_REFRESH_TOKEN_DAYS` (new setting, default 30). Not remembered: a session cookie and a
fixed 12-hour server expiry. The Google/GitHub landing and sign-up are remembered.
**Owner decision (2026-10-22): the lifetime is fixed, not sliding.** A family's expiry is set
when the sign-in starts and a rotated row inherits it, so refreshing never extends a sign-in; a
remembered cookie's `Max-Age` after a refresh is the time left.

**D3 — A new store, `refresh_tokens`.** The usual triad (in-memory, SQLite in the shared
`cairn.db`, Postgres with `PgSchemaInit`) selected by `CAIRN_STORAGE`; no per-store path knob.
Columns: `token_hash` (PK), `family_id`, `sub`, `account_id`, `remember`, `issued_at`,
`expires_at`, `used_at`, `replaced_by`, `revoked_at`; indexed on `family_id` and `sub`. Real
columns, not a JSON blob, because rotation is a compare-and-set. No workspace column: a
sign-in belongs to a user. Module: `src/cairn/core/refresh_tokens.py`.

**D4 — `POST /auth/refresh` rotates.** It reads the cookie and requires the header
`X-Cairn-Refresh: 1` and an `Origin` (else `Referer`) whose host is the request's `Host` or
`CAIRN_PUBLIC_URL`'s host (403 otherwise). The token must be known, unexpired and unrevoked,
and its user must still be able to sign in: the env admin while it is still configured, or a
store user that exists, is not disabled and is still a member of an account. It then marks
the row used (`UPDATE … WHERE used_at IS NULL AND revoked_at IS NULL`), issues the next row in
the same family, sets the new cookie and returns `{access_token, token_type}` as `/token`
does, with the claims re-minted by the same builder (`identity_claims`; the account is the
live membership, as `/token` resolves it). Presenting a token that was already rotated is
reuse: the whole family is revoked, the cookie cleared, 401. Every 401 refusal clears the
cookie; a 403 (header or origin) or a 400 (auth off) leaves it alone. Reuse, and revoking a
family whose user may no longer sign in, log a WARNING naming the user (`sub`) and the first 8
characters of the family id, never the token or its hash.

**D5 — `POST /auth/logout`** sits behind the same guard as refresh (one helper: the
`X-Cairn-Refresh: 1` header and the same-origin check, 403 otherwise), then revokes the
cookie's family and clears the cookie: 204 whether or not the cookie named a live sign-in, and
with no access token needed.

**D6 — Revocation on account changes.** `account.set_password` and
`account.set_disabled(…, True)` revoke every family of the user. The platform's admin routes
call these functions, so they inherit it. There is no user delete (#918 keeps records).

**D7 — Auth off.** No cookie is set; `/auth/refresh` and `/auth/logout` answer as `/token`
does with auth off (400, "auth is disabled").

**D8 — The CLI is unchanged.** `cairn remote login` keeps using the access token from the
body and ignores the cookie.

## Implementation choices the approval left open
- The family lifetime is absolute (a rotated row inherits `expires_at`); a remembered cookie's
  `Max-Age` after a refresh is the time left.
- A store failure while *issuing* a cookie at sign-in is logged and the sign-in succeeds
  without a cookie (the access token is the sign-in). A store failure while refreshing,
  revoking or signing out is an error, never a silent pass.
- `/auth/refresh` and `/auth/logout` are public paths in the bearer guard: the credential is
  the cookie. **Owner-approved 2026-10-22:** both are stated exemptions in the write-route
  workspace guard (`tests/core/test_write_route_workspace_guard.py` `EXEMPT`), like `/token`:
  they act in no workspace.
- Rotation writes the next row before the compare-and-set on the presented one. A loser's
  revocation runs only after the winner's compare-and-set, so it always finds the winner's new
  row and revokes it too; written after, the winner's row could land behind the revocation and
  keep the family alive (two concurrent rotations are tested on every backend).

## Consequences
- New API: `POST /auth/refresh`, `POST /auth/logout`; `POST /token` gains the `remember` form
  field and a `Set-Cookie` header (response body unchanged). New setting
  `CAIRN_REFRESH_TOKEN_DAYS`. New table `refresh_tokens`.
- The console can stay signed in for the remembered lifetime while access tokens stay short,
  and signing out ends the sign-in on the server.

**Known limits.**
- An access token already issued stays valid until it expires (at most
  `CAIRN_ACCESS_TOKEN_EXPIRE_MINUTES`) after sign-out, a password change or disabling.
- Rotating `CAIRN_JWT_SECRET` does not end refresh families (the tokens are random, not
  signed); deleting the `refresh_tokens` rows does. Changing the env admin's password does
  not revoke its families (it has no user record); removing the env admin does.
- Two concurrent refreshes with the same cookie (two tabs) are reuse by D4 and end the
  sign-in: the console must make one refresh at a time.
- Expired rows are never deleted; the table grows by one row per refresh.
- A store user with no account membership can sign in with `/token` but cannot refresh.
- The same-origin check compares hosts; a development proxy that rewrites `Host` (for
  example Vite's `changeOrigin`) needs `CAIRN_PUBLIC_URL` set to the browser's origin.
