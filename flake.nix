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
            llvmToolsBin = "${pkgs.llvmPackages.llvm}/bin";
            darwinLdFlags = pkgs.lib.optionalString pkgs.stdenv.isDarwin "-L${pkgs.darwin.libiconv}/lib";
            darwinRustFlags = pkgs.lib.optionalString pkgs.stdenv.isDarwin "-L native=${pkgs.darwin.libiconv}/lib";
            coveragePackages = basePackages ++ [
              pkgs.llvmPackages.llvm
            ];
            mkApp =
              name:
              {
                runtimeInputs ? basePackages,
                text,
              }:
              let
                script = pkgs.writeShellApplication {
                  inherit name;
                  inherit runtimeInputs;
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
              coveragePackages
              darwinLdFlags
              darwinRustFlags
              includePath
              libraryPath
              llvmToolsBin
              mkApp
              pkgs
              rustToolchain
              ;
          }
        );
    in
    {
      apps = forAllSystems (
        {
          coveragePackages,
          llvmToolsBin,
          mkApp,
          ...
        }:
        rec {
          default = check;
          check = mkApp "check" {
            text = ''
              cargo metadata --format-version 1 --no-deps
              cargo check
            '';
          };
          coverage-report = mkApp "coverage-report" {
            runtimeInputs = coveragePackages;
            text = ''
              export PATH="$HOME/.cargo/bin:$PATH"
              cargo +nightly llvm-cov --version >/dev/null 2>&1 || {
                echo "cargo +nightly llvm-cov must be available to run coverage-report" >&2
                exit 1
              }
              export LLVM_COV="${llvmToolsBin}/llvm-cov"
              export LLVM_PROFDATA="${llvmToolsBin}/llvm-profdata"
              coverage_target_dir="$(mktemp -d "''${TMPDIR:-/tmp}/radrootsd-llvm-cov.XXXXXX")"
              trap 'rm -rf "$coverage_target_dir"' EXIT
              export CARGO_TARGET_DIR="$coverage_target_dir"
              mkdir -p target/coverage
              cargo +nightly llvm-cov clean --workspace
              cargo +nightly llvm-cov --workspace --all-features --branch --no-report
              cargo +nightly llvm-cov report --json --summary-only --output-path target/coverage/summary.json
              cargo +nightly llvm-cov report --lcov --output-path target/coverage/lcov.info
              cargo +nightly llvm-cov report --summary-only
              echo "coverage summary: target/coverage/summary.json"
              echo "coverage lcov: target/coverage/lcov.info"
            '';
          };
          fmt = mkApp "fmt" {
            text = ''
              cargo fmt --all --check
            '';
          };
          test = mkApp "test" {
            text = ''
              cargo test
            '';
          };
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
