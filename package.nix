# sshterm-server, with the page it embeds, built from this checkout.
{
  lib,
  buildGoModule,
  buildNpmPackage,
  go_1_27,
}:
let
  version = "0.11.7";

  # Only what the build reads, so a change to the docs or the flake doesn't rebuild it.
  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [
      ./docroot
      ./go
      ./xterm
    ];
  };

  # xterm.js and its addons, bundled into xterm.mjs as xterm/update.sh does.
  xterm = buildNpmPackage {
    pname = "sshterm-xterm";
    inherit version;
    src = "${src}/xterm";
    npmDepsHash = "sha256-7RTXRFnukReYFhuTwcXil377Hix8/uLqCTi99H+joTA=";
    npmFlags = [ "--ignore-scripts" ];
    buildPhase = ''
      runHook preBuild
      mkdir -p ../docroot
      node_modules/.bin/rollup -c rollup.config.js
      runHook postBuild
    '';
    installPhase = ''
      runHook preInstall
      mkdir -p $out
      cp ../docroot/xterm.mjs node_modules/@xterm/xterm/css/xterm.css $out/
      cp node_modules/@xterm/xterm/LICENSE $out/LICENSE.xterm.txt
      runHook postInstall
    '';
  };
in
(buildGoModule.override { go = go_1_27; }) {
  pname = "sshterm-server";
  inherit version src;
  modRoot = "go";
  vendorHash = "sha256-+xIzIReN5WkkKXGcfDMB9VGCgy9fYwpRWSQoo585x5M=";
  env.CGO_ENABLED = 0;

  # The page, as go/build.sh makes it: the client compiled to WebAssembly, Go's loader for it,
  # and xterm.js, next to the docroot's own files. The server embeds it. Not in preBuild,
  # which buildGoModule also runs when it fetches the Go modules.
  postConfigure = ''
    GOOS=js GOARCH=wasm go build -trimpath -ldflags="-s -w" -o ../docroot/ssh.wasm .
    cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" ${xterm}/* ../docroot/
    rm -rf sshterm-server/internal/docroot
    mkdir -p sshterm-server/internal/docroot
    cp -r ../docroot/* sshterm-server/internal/docroot/
    rm -f sshterm-server/internal/docroot/tests.* sshterm-server/internal/docroot/config.json*
  '';
  subPackages = [ "sshterm-server" ];
  ldflags = [
    "-s"
    "-w"
  ];
  # The server's tests and the resume package's. The page's own need a browser.
  checkPhase = ''
    runHook preCheck
    go test ./sshterm-server/... ./internal/resume/...
    runHook postCheck
  '';
}
