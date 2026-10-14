#!/usr/bin/env bash
# Bundle Fedora rpmsign and non-glibc shared libraries for the UBI e2e image.
set -euo pipefail

bundle=/opt/rpmsign
mkdir -p "$bundle/usr/bin" "$bundle/lib64"
cp -a /usr/bin/rpmsign "$bundle/usr/bin/"
cp -a /usr/lib64/librpmsign.so* "$bundle/lib64/"

is_skip() {
    case "$1" in
    /lib64/libc.so* | /lib64/libdl.so* | /lib64/libm.so* | /lib64/libpthread.so* | /lib64/librt.so* | /lib64/libresolv.so* | /lib64/libz.so* | /lib64/libgcc_s.so*)
        return 0
        ;;
    esac
    return 1
}

queue=("$bundle/usr/bin/rpmsign")
for f in "$bundle/lib64"/librpmsign.so.*; do
    queue+=("$f")
done

declare -A seen=()
deps_file=$(mktemp)
trap 'rm -f "$deps_file"' EXIT

while ((${#queue[@]} > 0)); do
    obj="${queue[0]}"
    queue=("${queue[@]:1}")
    [[ -f "$obj" ]] || continue
    ldd "$obj" 2>/dev/null | awk '/=>/ {print $3}' >"$deps_file"
    while IFS= read -r lib; do
        [[ -z "$lib" || ! -f "$lib" ]] && continue
        is_skip "$lib" && continue
        dest="$bundle/lib64/$(basename "$lib")"
        [[ -n "${seen[$dest]:-}" ]] && continue
        seen[$dest]=1
        cp -a "$lib" "$dest"
        queue+=("$dest")
    done <"$deps_file"
done

# librpmsign links libimaevm; ensure it is present even if ldd did not resolve it during bundling.
shopt -s nullglob
for lib in /usr/lib64/libimaevm.so*; do
    cp -a "$lib" "$bundle/lib64/"
done
if [[ ! -e "$bundle/lib64/libimaevm.so.5" ]]; then
    echo "bundle-rpmsign: libimaevm.so.5 missing (install ima-evm-utils-libs)" >&2
    exit 1
fi
