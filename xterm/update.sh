#!/bin/bash -e

cd $(dirname $0)

npm ci --ignore-scripts

cp node_modules/@xterm/xterm/css/xterm.css ../docroot/xterm.css
cp node_modules/@xterm/xterm/LICENSE ../docroot/LICENSE.xterm.txt
# @xterm/addon-clipboard's bundle includes js-base64.
cp node_modules/js-base64/LICENSE.md ../docroot/LICENSE.js-base64.txt

npx rollup -c rollup.config.js
