#!/bin/bash
# Ensure the pixi env bin is at the front of PATH (activation.env PATH is evaluated
# before CONDA_PREFIX is set, so the env bin gets dropped — fix it here).
if [[ -n "$CONDA_PREFIX" ]]; then
    export PATH="$CONDA_PREFIX/bin:$PATH"
fi

if [[ "$(uname)" != "Darwin" ]]; then
    return 0 2>/dev/null || exit 0
fi

LIBTILEDB="$CONDA_PREFIX/lib/libtiledb.dylib"

if [[ ! -f "$LIBTILEDB" ]]; then
    return 0 2>/dev/null || exit 0
fi

# Change the install name of the pixi libtiledb.dylib to its absolute path so that
# macOS dyld treats it as distinct from any system-installed libtiledb.dylib and
# does not deduplicate them by the shared "@rpath/libtiledb.dylib" install name.
CURRENT_ID=$(otool -D "$LIBTILEDB" | tail -1)
if [[ "$CURRENT_ID" != "$LIBTILEDB" ]]; then
    install_name_tool -id "$LIBTILEDB" "$LIBTILEDB" 2>/dev/null
    codesign --force --sign - "$LIBTILEDB" 2>/dev/null
fi

# For each Python extension in the tiledb package that still uses @rpath/libtiledb.dylib,
# rewrite the reference to the absolute pixi path so macOS loads the right library.
TILEDB_PKG="$CONDA_PREFIX/lib/python3.12/site-packages/tiledb"
for SO in "$TILEDB_PKG"/libtiledb.cpython-312-darwin.so "$TILEDB_PKG"/main.cpython-312-darwin.so; do
    [[ -f "$SO" ]] || continue
    # Already points to the pixi libtiledb? Nothing to do.
    if otool -L "$SO" | grep -qF "$LIBTILEDB"; then
        continue
    fi
    CURRENT_REF=$(otool -L "$SO" | awk '/libtiledb\.dylib/{print $1; exit}')
    [[ -n "$CURRENT_REF" ]] || continue
    install_name_tool -change "$CURRENT_REF" "$LIBTILEDB" "$SO" 2>/dev/null
    codesign --force --sign - "$SO" 2>/dev/null
done
