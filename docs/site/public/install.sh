#!/bin/sh
# cairn installer — https://docs.cairndev.sh/install.sh (#2430)
#
#   curl -LsSf https://docs.cairndev.sh/install.sh | sh
#   curl -LsSf https://docs.cairndev.sh/install.sh | CAIRN_EXTRAS=hello,rag sh
#   curl -LsSf https://docs.cairndev.sh/install.sh | CAIRN_VERSION=0.3.0 sh
#
# Installs uv if it is missing (astral.sh's own installer, into ~/.local/bin, no sudo; like any uv
# install it adds that directory to your shell's PATH), then installs — or upgrades — the
# `cairndev` distribution from PyPI as a uv tool: `cairn` lands in ~/.local/bin, in an environment
# of its own, isolated from every other Python project. Re-run it to upgrade.
#
#   CAIRN_EXTRAS   comma-separated extras, e.g. hello,rag,metaflow (default: none)
#   CAIRN_VERSION  an exact version to pin (default: the latest release)
#   CAIRN_PYTHON   the Python uv provisions for the tool (default: 3.13)
#   CAIRN_RELEASE  also look for cairn's wheels in this GitHub Release and pin cairndev and
#                  cairn-client to it (default: unset, PyPI only); needed for versions before
#                  0.1.3, which are not on PyPI; third-party packages still come from PyPI
#
# Uninstall: uv tool uninstall cairndev
#
# The whole body is one function called on the last line, so a download cut short runs nothing.
set -eu

main() {
  LC_ALL=C
  export LC_ALL

  extras="${CAIRN_EXTRAS:-}"
  version="${CAIRN_VERSION:-}"
  python="${CAIRN_PYTHON:-3.13}"

  # The values are spliced into a package spec, so only the characters a spec can hold pass.
  case "$extras" in *[!a-z0-9,-]*) die "CAIRN_EXTRAS may hold only lowercase letters, digits, '-' and ',' (got: $extras)";; esac
  case "$version" in *[!A-Za-z0-9.+-]*) die "CAIRN_VERSION is not a version (got: $version)";; esac
  case "$python" in ""|*[!0-9.]*) die "CAIRN_PYTHON is not a Python version (got: $python)";; esac
  # cairn comes from PyPI. CAIRN_RELEASE=<version> also looks for cairn's wheels in that GitHub
  # Release (versions before 0.1.3, or a release whose PyPI upload is pending).
  release="${CAIRN_RELEASE:-}"
  case "$release" in *[!A-Za-z0-9.+-]*) die "CAIRN_RELEASE is not a version (got: $release)";; esac

  if ! command -v uv >/dev/null 2>&1; then
    install_uv
  fi

  spec="cairndev"
  [ -n "$extras" ] && spec="cairndev[$extras]"
  [ -n "$version" ] && spec="$spec==$version"

  if [ -n "$release" ]; then
    # Pinned to the release while the wheels come from GitHub: a higher version on PyPI must not
    # win over the release that was asked for.
    [ -n "$version" ] || spec="$spec==$release"
    links="https://github.com/cairndev-sh/cairn/releases/expanded_assets/v$release"
    say "installing $spec (Python $python) as a uv tool, wheels from $links"
    uv tool install --upgrade --force --python "$python" --find-links "$links" --with "cairn-client==$release" "$spec"
  else
    say "installing $spec (Python $python) as a uv tool"
    uv tool install --upgrade --force --python "$python" "$spec"
  fi

  say "done — run: cairn --help"
  say "if 'cairn' is not found, run 'uv tool update-shell' and open a new shell"
}

install_uv() {
  say "uv not found — installing it with astral.sh's installer (https://docs.astral.sh/uv/)"
  if command -v curl >/dev/null 2>&1; then fetch="curl"
  elif command -v wget >/dev/null 2>&1; then fetch="wget"
  else die "neither curl nor wget is available to fetch uv; install uv first: https://docs.astral.sh/uv/getting-started/installation/"
  fi
  installer="$(mktemp)" || die "mktemp is needed to stage uv's installer"
  trap 'rm -f "$installer"' EXIT
  # Downloaded to a file first: a failed or partial fetch stops here instead of piping nothing to sh.
  if [ "$fetch" = curl ]; then
    curl -LsSf https://astral.sh/uv/install.sh -o "$installer" || die "could not download uv's installer from astral.sh"
  else
    wget -qO "$installer" https://astral.sh/uv/install.sh || die "could not download uv's installer from astral.sh"
  fi
  [ -s "$installer" ] || die "uv's installer downloaded empty from astral.sh"
  sh "$installer" || die "uv's installer failed"
  # where astral's installer puts uv: UV_INSTALL_DIR, else XDG_BIN_HOME, else ~/.local/bin
  PATH="${UV_INSTALL_DIR:+$UV_INSTALL_DIR:}${XDG_BIN_HOME:+$XDG_BIN_HOME:}$HOME/.local/bin:$PATH"
  command -v uv >/dev/null 2>&1 || die "uv was installed but is not on PATH; open a new shell and re-run this installer"
}

say() { printf 'cairn-install: %s\n' "$*"; }
die() { printf 'cairn-install: error: %s\n' "$*" >&2; exit 1; }

main "$@"
