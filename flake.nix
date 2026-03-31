{
  description = "radrootsd";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    { nixpkgs, rust-overlay, ... }:
    let
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];
      forAllSystems =
        f:
        nixpkgs.lib.genAttrs systems (
          system:
          let
            pkgs = import nixpkgs {
              inherit system;
              overlays = [ rust-overlay.overlays.default ];
            };
            rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
            basePackages =
              [
                pkgs.git
                rustToolchain
                pkgs.clang
                pkgs.llvmPackages.libclang
                pkgs.libsodium
                pkgs.openssl
                pkgs.pkg-config
                pkgs.sqlite
              ]
              ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
                pkgs.darwin.libiconv
              ];
            libraryPath = pkgs.lib.makeLibraryPath basePackages;
            includePath = pkgs.lib.makeSearchPathOutput "dev" "include" basePackages;
            darwinLdFlags = pkgs.lib.optionalString pkgs.stdenv.isDarwin "-L${pkgs.darwin.libiconv}/lib";
            darwinRustFlags = pkgs.lib.optionalString pkgs.stdenv.isDarwin "-L native=${pkgs.darwin.libiconv}/lib";
            mkApp =
              name: text:
              let
                script = pkgs.writeShellApplication {
                  inherit name;
                  runtimeInputs = basePackages;
                  text = ''
                    set -euo pipefail
                    repo_root="$(git rev-parse --show-toplevel)"
                    cd "$repo_root"
                    export LIBCLANG_PATH="${pkgs.llvmPackages.libclang.lib}/lib"
                    export LIBRARY_PATH="${libraryPath}:''${LIBRARY_PATH:-}"
                    export DYLD_FALLBACK_LIBRARY_PATH="${libraryPath}:''${DYLD_FALLBACK_LIBRARY_PATH:-}"
                    export LDFLAGS="${darwinLdFlags} ''${LDFLAGS:-}"
                    export NIX_LDFLAGS="${darwinLdFlags} ''${NIX_LDFLAGS:-}"
                    export RUSTFLAGS="${darwinRustFlags} ''${RUSTFLAGS:-}"
                    export CPATH="${includePath}:''${CPATH:-}"
                    ${text}
                  '';
                };
              in
              {
                type = "app";
                program = "${script}/bin/${name}";
              };
          in
          f {
            inherit
              basePackages
              darwinLdFlags
              darwinRustFlags
              includePath
              libraryPath
              mkApp
              pkgs
              rustToolchain
              ;
          }
        );
    in
    {
      apps = forAllSystems (
        { mkApp, ... }:
        rec {
          default = check;
          check = mkApp "check" ''
            cargo metadata --format-version 1 --no-deps
            cargo check
          '';
          fmt = mkApp "fmt" ''
            cargo fmt --all --check
          '';
          test = mkApp "test" ''
            cargo test
          '';
        }
      );

      devShells = forAllSystems (
        {
          basePackages,
          darwinLdFlags,
          darwinRustFlags,
          includePath,
          libraryPath,
          pkgs,
          ...
        }:
        {
          default = pkgs.mkShell {
            packages = basePackages;
            shellHook = ''
              export LIBCLANG_PATH="${pkgs.llvmPackages.libclang.lib}/lib"
              export LIBRARY_PATH="${libraryPath}:''${LIBRARY_PATH:-}"
              export DYLD_FALLBACK_LIBRARY_PATH="${libraryPath}:''${DYLD_FALLBACK_LIBRARY_PATH:-}"
              export LDFLAGS="${darwinLdFlags} ''${LDFLAGS:-}"
              export NIX_LDFLAGS="${darwinLdFlags} ''${NIX_LDFLAGS:-}"
              export RUSTFLAGS="${darwinRustFlags} ''${RUSTFLAGS:-}"
              export CPATH="${includePath}:''${CPATH:-}"
            '';
          };
        }
      );
    };
}
