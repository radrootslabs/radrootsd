# Nix

This repository uses Nix as the canonical local development and validation environment.

## Enter The Shell

```bash
nix develop
```

## Command Map

```bash
nix run .#fmt
nix run .#check
nix run .#test
```

Use `nix develop` before running narrower ad hoc cargo commands from this repo root.
