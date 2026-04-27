# Repository Guidelines

## Project Structure & Module Organization
This repository is a small Rust binary crate. The manifest is in `Cargo.toml`, the lockfile is `Cargo.lock`, and the current entrypoint is `src/main.rs`. Build artifacts are written to `target/` and should not be edited manually.

As the codebase grows, keep runtime code in `src/` and split features into focused modules such as `src/forwarder.rs` or `src/net/mod.rs`. Place integration tests in `tests/` and keep test fixtures near the tests that use them.

## Build, Test, and Development Commands
- `cargo run`: build and run the binary locally.
- `cargo build`: compile the crate without running it.
- `cargo test`: run unit and integration tests. This currently succeeds with `0` tests and should remain green as coverage is added.
- `cargo fmt -- --check`: verify standard Rust formatting.

Run commands from the repository root:

```powershell
cargo build
cargo test
```

## Coding Style & Naming Conventions
Follow standard Rust style and let `rustfmt` define layout. Use 4-space indentation, keep functions and modules in `snake_case`, types and traits in `PascalCase`, and constants in `SCREAMING_SNAKE_CASE`.

Prefer small modules with explicit responsibilities. Keep `main.rs` thin by moving networking or forwarding logic into dedicated files under `src/`.

## Testing Guidelines
Prefer unit tests next to the code they cover using `#[cfg(test)]`, and add integration tests under `tests/` for end-to-end forwarding behavior. Name tests after observable behavior, for example `forwards_bytes_between_sockets`.

Every functional change should include tests when practical. Run `cargo test` before opening a pull request.

## Commit & Pull Request Guidelines
There is no existing commit history yet, so no repository-specific convention can be inferred. Use short, imperative commit subjects such as `Add TCP listener setup` or `Extract forwarding loop`.

Pull requests should include a brief summary, the reason for the change, and the verification steps you ran. Include terminal output or screenshots only when they clarify behavior.
