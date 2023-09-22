#!/usr/bin/env bash

# Copyright © 2021 Joel Baranick <jbaranick@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BUILD_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
BINARY_DIR="$BUILD_DIR/.bin"
VERSION=$(cat $BUILD_DIR/.version)
REVISION="$(git rev-parse HEAD)"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
USER="${USER}"
HOST="$(hostname)"
BUILD_DATE="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

function verbose() { echo -e "$*"; }
function error() { echo -e "ERROR: $*" 1>&2; }
function fatal() { echo -e "ERROR: $*" 1>&2; exit 1; }
function pushd () { command pushd "$@" > /dev/null; }
function popd () { command popd > /dev/null; }

function trap_add() {
  localtrap_add_cmd=$1; shift || fatal "${FUNCNAME} usage error"
  for trap_add_name in "$@"; do
    trap -- "$(
      extract_trap_cmd() { printf '%s\n' "$3"; }
      eval "extract_trap_cmd $(trap -p "${trap_add_name}")"
      printf '%s\n' "${trap_add_cmd}"
    )" "${trap_add_name}" || fatal "unable to add to trap ${trap_add_name}"
  done
}
declare -f -t trap_add

function get_platform() {
  local unameOut="$(uname -s)"
  case "${unameOut}" in
    Linux*)
      echo "linux"
    ;;
    Darwin*)
      echo "darwin"
    ;;
    *)
      echo "Unsupported machine type :${unameOut}"
      exit 1
    ;;
  esac
}

PLATFORM=$(get_platform)
GOX="gox"
GOCILINTER=${BINARY_DIR}/golangci-lint
GOCILINTER_URL=https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh
GOCILINTER_VERSION=v1.43.0

function download_gocilinter() {
  if [[ ! -f "$GOCILINTER" ]]; then
    verbose "   --> $GOCILINTER"
    curl -sSfL $GOCILINTER_URL | sh -s -- -b ${BINARY_DIR} $GOCILINTER_VERSION || fatal "failed to download '$GOCILINTER_URL': $?"
  fi
}

function download_gox() {
  if [[ ! -x "$(command -v $GOX)" ]]; then
    verbose "   --> $GOX"
    go install github.com/mitchellh/gox || fatal "go get 'github.com/mitchellh/gox' failed: $?"
    goenv rehash
  fi
}

function download_binaries() {
  download_gox || fatal "failed to download 'gox': $?"
  download_gocilinter || fatal "failed to download 'gocilinter': $?"
  export PATH=$PATH:${BINARY_DIR}
}

function usage() {
  echo "Usage: build.sh [OPTIONS ...]"
  echo "Builds the binary for all supported platforms."
  echo ""
  echo "Options:"
  echo "    --help:        display this help"
  echo ""
}

function parse_args() {
  for var in "${@}"; do
    case "$var" in
      --help)
        usage
        exit 0
      ;;
    esac
  done
}

function run() {
  parse_args "$@"

  local revision=`git rev-parse HEAD`
  local branch=`git rev-parse --abbrev-ref HEAD`
  local host=`hostname`
  local buildDate=`date -u +"%Y-%m-%dT%H:%M:%SZ"`
  local go_version="$(cat ${BUILD_DIR}/.go-version)"
  go version | grep -q "go version go${go_version%*.0} " || fatal "go version is not ${go_version%*.0}"

  if [[ -z "$TRAVIS" ]]; then
    verbose "Cleanup dist..."
    rm -rf dist/*
  fi

  verbose "Fetching binaries..."
  download_binaries

  verbose "Linting source..."
  ${GOCILINTER} run --verbose *.go || fatal "gocilinter failed: $?"

  verbose "Checking licenses..."
  local licRes=$(
  for file in $(find . -type f -iname '*.go' ! -path './vendor/*'); do
    head -n3 "${file}" | grep -Eq "(Copyright|generated|GENERATED)" || error "  Missing license in: ${file}"
  done;)
  if [[ -n "${licRes}" ]]; then
  	fatal "license header checking failed:\n${licRes}"
  fi

  XC_ARCH=${XC_ARCH:-"386 amd64 arm arm64"}
  XC_OS=${XC_OS:-"darwin linux windows"}

  verbose "Building binaries..."
  ${GOX} -os="${XC_OS}" -arch="${XC_ARCH}" -os="!darwin !windows" -tags 'osusergo netgo static_build' -ldflags "-d -s -w -extldflags \"-fno-PIC -static\" -X github.com/kadaan/mqttcli/version.Version=$VERSION -X github.com/kadaan/mqttcli/version.Revision=$REVISION -X github.com/kadaan/mqttcli/version.Branch=$BRANCH -X github.com/kadaan/mqttcli/version.BuildUser=$USER -X github.com/kadaan/mqttcli/version.BuildHost=$HOST -X github.com/kadaan/mqttcli/version.BuildDate=$BUILD_DATE" -output="dist/{{.Dir}}_{{.OS}}_{{.Arch}}" || fatal "gox failed: $?"

  verbose "Building binaries..."
  ${GOX} -os="${XC_OS}" -arch="${XC_ARCH}" -os="!linux" -osarch="!darwin/arm !darwin/arm64 !darwin/386" -tags 'osusergo netgo static_build' -ldflags "-s -w -extldflags \"-fno-PIC -static\" -X github.com/kadaan/mqttcli/version.Version=$VERSION -X github.com/kadaan/mqttcli/version.Revision=$REVISION -X github.com/kadaan/mqttcli/version.Branch=$BRANCH -X github.com/kadaan/mqttcli/version.BuildUser=$USER -X github.com/kadaan/mqttcli/version.BuildHost=$HOST -X github.com/kadaan/mqttcli/version.BuildDate=$BUILD_DATE" -output="dist/{{.Dir}}_{{.OS}}_{{.Arch}}" || fatal "gox failed: $?"


  if [[ -n "$TRAVIS" ]]; then
    verbose "Creating archives..."
    cd dist
    set -x
    for f in *; do
      local filename=$(basename "$f")
      local extension="${filename##*.}"
      local filename="${filename%.*}"
      if [[ "$filename" != "$extension" ]] && [[ -n "$extension" ]]; then
        extension=".$extension"
      else
        extension=""
      fi
      local archivename="$filename.tar.gz"
      verbose "   --> $archivename"
      local genericname="mqttcli$extension"
      mv -f "$f" "$genericname"
      tar -czf ${archivename} "$genericname"
      rm -rf "$genericname"
    done
  fi
}

run "$@"
