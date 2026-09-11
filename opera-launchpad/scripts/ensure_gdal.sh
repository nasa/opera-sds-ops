#!/usr/bin/env bash
# Ensures a native GDAL install is present, then pip-installs the exact
# matching Python bindings (the `GDAL`/`osgeo` package) into a tool's own
# venv. GDAL's Python bindings must match the native library's version
# precisely, so this always detects that version rather than pinning one.
#
# Usage (run from the tool's own working directory, as a post_setup step):
#   $OPERA_LAUNCHPAD_ROOT/scripts/ensure_gdal.sh [path-to-pip]
#
# path-to-pip defaults to auto-detecting venv/bin/pip (macOS/Linux) or
# venv/Scripts/pip.exe (Windows/Git Bash) relative to the current directory.
set -uo pipefail

detect_pip() {
    if [ -x "venv/bin/pip" ]; then
        echo "venv/bin/pip"
    elif [ -x "venv/Scripts/pip.exe" ]; then
        echo "venv/Scripts/pip.exe"
    else
        echo "venv/bin/pip"  # let the eventual pip install fail loudly if this guess is wrong
    fi
}

PIP="${1:-$(detect_pip)}"

# gdal-config (macOS/Linux) reports the version directly. Windows installs
# (conda, Chocolatey) typically ship gdalinfo instead, so fall back to
# parsing "GDAL 3.9.1, released 2024/06/25" out of `gdalinfo --version`.
find_gdal_version() {
    local gdal_config
    gdal_config="$(command -v gdal-config 2>/dev/null)"
    if [ -z "$gdal_config" ]; then
        for cellar in /opt/homebrew/Cellar/gdal /usr/local/Cellar/gdal; do
            gdal_config="$(find "$cellar" -name gdal-config -not -path '*/bash_completion.d/*' 2>/dev/null | sort -V | tail -1)"
            [ -n "$gdal_config" ] && break
        done
    fi
    if [ -n "$gdal_config" ]; then
        export PATH="$(dirname "$gdal_config"):$PATH"
        "$gdal_config" --version
        return
    fi

    if command -v gdalinfo >/dev/null 2>&1; then
        gdalinfo --version | sed -n 's/^GDAL \([0-9.]*\).*/\1/p'
        return
    fi
}

install_native_gdal() {
    case "$(uname -s)" in
        Darwin)
            if command -v brew >/dev/null 2>&1; then
                echo "Installing GDAL via Homebrew..."
                brew install gdal
            else
                echo "ERROR: Homebrew not found. Install GDAL manually: https://gdal.org/download.html" >&2
                return 1
            fi
            ;;
        Linux)
            if command -v apt-get >/dev/null 2>&1; then
                echo "Installing GDAL via apt-get (may prompt for sudo password)..."
                sudo apt-get update && sudo apt-get install -y gdal-bin libgdal-dev
            elif command -v dnf >/dev/null 2>&1; then
                echo "Installing GDAL via dnf (may prompt for sudo password)..."
                sudo dnf install -y gdal gdal-devel
            elif command -v yum >/dev/null 2>&1; then
                echo "Installing GDAL via yum (may prompt for sudo password)..."
                sudo yum install -y gdal gdal-devel
            elif command -v pacman >/dev/null 2>&1; then
                echo "Installing GDAL via pacman (may prompt for sudo password)..."
                sudo pacman -S --noconfirm gdal
            else
                echo "ERROR: No supported package manager found (apt-get/dnf/yum/pacman)." >&2
                echo "Install GDAL manually: https://gdal.org/download.html" >&2
                return 1
            fi
            ;;
        MINGW*|MSYS*|CYGWIN*)
            if command -v conda >/dev/null 2>&1; then
                echo "Installing GDAL via conda..."
                conda install -y -c conda-forge gdal
            elif command -v choco >/dev/null 2>&1; then
                echo "Installing GDAL via Chocolatey..."
                choco install gdal -y
            else
                echo "ERROR: Neither conda nor Chocolatey found." >&2
                echo "Install GDAL via conda (conda install -c conda-forge gdal) or" >&2
                echo "Chocolatey (choco install gdal), then re-run setup." >&2
                return 1
            fi
            ;;
        *)
            echo "ERROR: Unsupported OS '$(uname -s)' for automatic GDAL install." >&2
            echo "Install GDAL manually: https://gdal.org/download.html" >&2
            return 1
            ;;
    esac
}

GDAL_VERSION="$(find_gdal_version)"
if [ -z "$GDAL_VERSION" ]; then
    install_native_gdal || exit 1
    GDAL_VERSION="$(find_gdal_version)"
fi

if [ -z "$GDAL_VERSION" ]; then
    echo "ERROR: Could not detect a native GDAL version after install attempt." >&2
    exit 1
fi

echo "Found native GDAL $GDAL_VERSION"
"$PIP" install "GDAL==$GDAL_VERSION"
