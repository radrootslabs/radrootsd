{
  description = "Radroots public runtime daemon";

  inputs = {
    # Crane 0.23+ currently asks nixpkgs' Cargo vendor helper to fetch
    # semver-build-metadata crate versions through the crates.io API. That
    # endpoint rejects the literal `+`; the immutable v0.22.0 input avoids
    # that upstream fetch defect while preserving the same locked sources.
    crane.url = "github:ipetkov/crane/01bc1d404a51a0a07e9d8759cd50a7903e218c82";
    lib = {
      url = "github:radrootslabs/lib/055096853fca95e15d0f813d33a14aca13be3881";
      inputs.crane.follows = "crane";
    };
    nixpkgs.follows = "lib/nixpkgs";
    rust-overlay.follows = "lib/rust-overlay";
  };

  outputs =
    {
      crane,
      lib,
      nixpkgs,
      rust-overlay,
      ...
    }:
    let
      systems = lib.lib.supportedSystems;
      forAllSystems =
        function:
        builtins.listToAttrs (
          map (system: {
            name = system;
            value = function system;
          }) systems
        );
      daemonOutputs =
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };
          helpers = lib.lib.mkServiceHelpers system;
          toolchain = helpers.mkToolchain {
            rustToolchainFile = ./rust-toolchain.toml;
          };
          nativeInputs = helpers.mkNativeInputs { };
          craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
          source = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter =
              path: type:
              craneLib.filterCargoSources path type
              || pkgs.lib.hasSuffix ".json" (baseNameOf path)
              || pkgs.lib.hasSuffix ".txt" (baseNameOf path)
              || baseNameOf path == "flake.nix"
              || baseNameOf path == "README";
            name = "radrootsd-source";
          };
          commonArgs = {
            src = source;
            cargoLock = ./Cargo.lock;
            strictDeps = true;
            nativeBuildInputs = nativeInputs.nativeBuildInputs;
            buildInputs = nativeInputs.buildInputs;
            env = nativeInputs.environment;
            doCheck = false;
          };
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
          package = craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts;
              pname = "radrootsd";
              version = "0.1.0";
              CARGO_PROFILE = "release";
              cargoExtraArgs = "--locked --package radrootsd --bin radrootsd";
            }
          );
          check = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              pname = "radrootsd-check";
              version = "1";
              buildPhaseCargoCommand = "cargo check --locked --package radrootsd --all-targets";
              installPhaseCommand = "mkdir -p $out";
            }
          );
          app = {
            type = "app";
            program = "${package}/bin/radrootsd";
            meta.description = "Run the built radrootsd daemon";
          };
        in
        {
          inherit app check package;
        };
    in
    {
      packages = forAllSystems (system: {
        default = (daemonOutputs system).package;
      });
      checks = forAllSystems (system: {
        default = (daemonOutputs system).check;
      });
      apps = forAllSystems (system: {
        default = (daemonOutputs system).app;
      });
    };
}
