#!/usr/bin/env sh
set -eu

# Pull a tagged mdbtools source tarball and unpack it into src/mdbtools.
VERSION="${1:-1.0.0}"
ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
SRC_DIR="$ROOT_DIR/src"
TARGET_DIR="$SRC_DIR/mdbtools"
URL_RELEASE="https://github.com/mdbtools/mdbtools/releases/download/v${VERSION}/mdbtools-${VERSION}.tar.gz"
URL_GH_ARCHIVE="https://github.com/mdbtools/mdbtools/archive/refs/tags/v${VERSION}.tar.gz"

mkdir -p "$SRC_DIR"

fetch() {
  tarball_path="$1"
  url="$2"

  if command -v curl >/dev/null 2>&1; then
    curl -fL "$url" -o "$tarball_path"
    return 0
  fi

  if command -v wget >/dev/null 2>&1; then
    wget -O "$tarball_path" "$url"
    return 0
  fi

  echo "ERROR: neither curl nor wget is available" >&2
  return 1
}

prune_non_build_files() {
  # Remove non-build and CI/documentation assets from vendored source.
  rm -rf "$TARGET_DIR/.github"
  rm -rf "$TARGET_DIR/api_docx"

  if [ -d "$TARGET_DIR/doc" ]; then
    find "$TARGET_DIR/doc" -mindepth 1 -maxdepth 1 \
      ! -name 'Makefile.in' \
      ! -name 'Makefile.am' \
      -exec rm -rf {} +
  fi

  rm -f "$TARGET_DIR/.gitignore"
  rm -f "$TARGET_DIR/.gitlab-ci.yml"
  rm -f "$TARGET_DIR/appveyor.yml"
  rm -f "$TARGET_DIR/AUTHORS"
  rm -f "$TARGET_DIR/HACKING"
  rm -f "$TARGET_DIR/HACKING.md"
  rm -f "$TARGET_DIR/INSTALL"
  rm -f "$TARGET_DIR/NEWS"
  rm -f "$TARGET_DIR/README.md"
  rm -f "$TARGET_DIR/TODO.md"
  rm -f "$TARGET_DIR/test_script.sh"
  rm -f "$TARGET_DIR/test_sql.sh"
  rm -f "$TARGET_DIR/m4/lt~obsolete.m4"
}

tmp_tarball="$(mktemp "${TMPDIR:-/tmp}/mdbtoolr-mdbtools-${VERSION}-XXXXXX.tar.gz")"
tmp_extract="$(mktemp -d "${TMPDIR:-/tmp}/mdbtoolr-src-XXXXXX")"
trap 'rm -rf "$tmp_extract"; rm -f "$tmp_tarball"' EXIT INT TERM

echo "[mdbtoolr] Downloading mdbtools v${VERSION} release tarball"
if ! fetch "$tmp_tarball" "$URL_RELEASE"; then
  echo "[mdbtoolr] Release tarball unavailable, falling back to GitHub archive"
  fetch "$tmp_tarball" "$URL_GH_ARCHIVE"
fi

rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR"

tar -xzf "$tmp_tarball" -C "$tmp_extract"

src_unpacked="$(find "$tmp_extract" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
if [ -z "$src_unpacked" ] || [ ! -d "$src_unpacked" ]; then
  echo "ERROR: failed to unpack mdbtools sources" >&2
  exit 1
fi

cp -R "$src_unpacked"/. "$TARGET_DIR"/
prune_non_build_files

if [ ! -x "$TARGET_DIR/configure" ]; then
  echo "ERROR: vendored source does not include configure; expected release tarball layout" >&2
  exit 1
fi

echo "[mdbtoolr] Vendored mdbtools source refreshed in $TARGET_DIR"
