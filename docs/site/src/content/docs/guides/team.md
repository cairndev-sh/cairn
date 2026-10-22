---
title: Set up your team
description: Turn sign-in on, create the first account, decide who can sign up, use workspaces and roles, and control who decides each gate.
---

This guide takes a deployment from a single admin login to a team working in
one place. It assumes a server you started yourself, or the Docker Compose
stack from [Install and deploy](/install/).

## 1. Turn sign-in on

With no JWT secret the API is open, which is fine only on a laptop. Set these
on the server and restart it:

```bash
CAIRN_JWT_SECRET=<openssl rand -hex 32>
CAIRN_API_USER=admin
CAIRN_API_PASSWORD=<choose one>    # or CAIRN_API_PASSWORD_HASH from `cairn remote hash-password`
```

Every request now needs a token. Sign in from the CLI; the token is stored in
your CLI profile:

```bash
cairn remote login -e https://cairn.example.com -u admin
```

Sign-up and `cairn remote hash-password` hash passwords with argon2, which the base install
carries (since 0.1.2; on 0.1.1 install the `auth` extra).

A console sign-in lasts longer than its access token (30 minutes,
`CAIRN_ACCESS_TOKEN_EXPIRE_MINUTES`). Signing in, signing up or signing in with
GitHub or Google also sets an httpOnly `cairn_refresh` cookie, and the console
trades it for a new access token with `POST /auth/refresh`. A remembered sign-in
lasts `CAIRN_REFRESH_TOKEN_DAYS` (default 30); `POST /token` with
`remember=false` gives a browser-session cookie the server honours for 12 hours.
A sign-in's lifetime is fixed when it starts; refreshing does not extend it.
`POST /auth/logout` ends the sign-in on the server (like a refresh, it needs the
`X-Cairn-Refresh: 1` header and must come from the console's own origin). Changing a user's password or
disabling them ends all their sign-ins. Set `CAIRN_PUBLIC_URL` to the address
people use: the cookie is `Secure` when it is https, and a refresh is accepted
only from that origin or the host the request was sent to.

## 2. Create the first account

People other than the configured admin sign up through the API:

```bash
curl -s https://cairn.example.com/auth/signup                 # is sign-up open, and which providers work
curl -s -X POST https://cairn.example.com/auth/signup \
  -H 'content-type: application/json' \
  -d '{"company": "Acme", "email": "ana@acme.com", "password": "…"}'
```

On a deployment with no accounts yet, the first person to sign up always gets
in, whatever the sign-up mode, and becomes the **owner** of a new account. The
account gets its first workspace, and the response carries a token, so
sign-up is also a sign-in. That is how the first owner exists on an
invite-only install.

## 3. Decide who else can sign up

`CAIRN_SIGNUP` on the server decides, after the first account:

| Mode | Who can sign up |
|---|---|
| `invite` (default) | nobody; people join with an invite link |
| `domain` | anyone whose work email matches an account's registered domain; they join that account as a `member` |
| `open` | anyone; each sign-up creates a new account |

Sign-up registers the founder's email domain on the new account, unless it is
a personal-mail domain such as gmail.com. So under `domain`, colleagues at
`acme.com` can sign up and land in Acme's account, and nobody joins through a
gmail.com address. An email already registered is refused.

Issuing invite links, managing users and changing an account's registered
domain are part of cairn-platform, as are the console's sign-up, setup and
settings pages. A person holding an invite link redeems it at
`POST /auth/accept-invite` on any deployment.

## 4. Sign in with GitHub or Google

Install the `oauth` extra, set `CAIRN_PUBLIC_URL`, register an OAuth app with
the callback `<CAIRN_PUBLIC_URL>/auth/oauth/github/callback` (or `google`),
and set `CAIRN_OAUTH_GITHUB_CLIENT_ID` and `CAIRN_OAUTH_GITHUB_CLIENT_SECRET`
(or the `GOOGLE` pair). `GET /auth/signup` then lists the provider, and
`/auth/oauth/github/start` begins the sign-in.

Signing in with a provider uses the verified email it returns. It doesn't
bypass the sign-up mode: under `invite`, an email with no account still needs
an invite.

## 5. Workspaces

A workspace is where pipelines are enabled and runs are recorded. Each account
sees only its own workspaces. List and create them over the API:

```bash
curl -s -H "authorization: Bearer $TOKEN" https://cairn.example.com/workspaces
```

Creating a workspace with an explicit `id` that already exists is refused
(`409` when your account holds it, `404` when another account does); a create
by name alone takes the name's first free slug (`ops-2`). Archiving or
restoring one (`DELETE` or `PATCH /workspaces/<id>`) needs an owner or admin of
the account that owns it, or the deployment's admin (`CAIRN_API_USER`);
another account's workspace answers `404`.

A request acts in a workspace when its path starts with
`/workspaces/<id>/`, or when it carries the `X-Cairn-Workspace` header. With
sign-in on, naming a workspace that does not exist answers `404`, the same as
naming one another account owns; create it first (`POST /workspaces`). So
`CAIRN_WORKSPACE`, if set, must name a workspace that exists (`default` always
does); the server warns at startup when it does not. On a multi-node deployment
the workspace store must be shared (Postgres): each node's SQLite store only
knows the workspaces created on it.

A request that names neither acts in the server's default workspace
(`CAIRN_WORKSPACE`, else `default`) if your account owns it, and otherwise in
your account's oldest active workspace, the one sign-in opens; a
`CAIRN_WORKSPACE` that does not exist sends every such request there. An
account with no active workspace gets `404` on every workspace-scoped route,
but can still sign in, list, create and restore its workspaces. The engine installs every
pipeline it finds into the `default` workspace when it starts.

## 6. Roles, and who decides a gate

Every member of an account has a role: `member`, `admin` or `owner` (the
person who created the account). A gate's `approvers:` names who may decide
it, as usernames or roles:

```yaml
  - gate: review
    depends_on: [stage]
    approvers: data-owners
```

Someone `approvers:` names may decide; an owner or admin always may. A name
that is neither a user nor a role matches nobody, so only an owner or admin can
decide that gate. The decision is recorded under the identity of the person
signed in. See [Gates and approvals](/concepts/gates/#who-decides).

## Next

- [Gates and approvals](/concepts/gates/): what approvers decide, and where.
- [Run your first pipeline](/guides/first-pipeline/)
- [Connect a provider](/guides/connect-a-provider/)
