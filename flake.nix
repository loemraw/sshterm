# The homelab branch: SSH Term with the changes ssh.loemra.com runs. The homelab's flake takes
# sshterm-server from here.
{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];
      forEachSystem = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      packages = forEachSystem (pkgs: rec {
        sshterm-server = pkgs.callPackage ./package.nix { };
        default = sshterm-server;
      });
    };
}
