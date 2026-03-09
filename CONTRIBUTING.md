# Contributing to DriftQ-Core

Thanks for your interest in contributing to DriftQ-Core

This project is actively evolving, but contributions are welcome — especially:
- Bug reports + minimal repros
- Docs improvements (README + `docs/v1/*`, `docs/v2/*`)
- Tests (unit + integration)
- Small, focused fixes (perf, correctness, DX)

If you’re proposing a larger feature or behavior change, please open an Issue first so we can align before you spend time on a PR.


## Quick links

- Project overview + quickstart: `README.md`
- v1 API reference: `docs/v1/v1-README.md`
- v2 foundations: `docs/v2/v2-README.md`


## Contribution boundaries (v1 vs v2)

DriftQ-Core has two tracks:

- **v1 broker (`/v1/*`) is stable.** If you change v1 behavior, please:
  - Include tests (unit/integration as appropriate)
  - Update docs (`docs/v1/*` and/or README) if the API/behavior changes
  - Call out compatibility implications (WAL, retry semantics, ack/nack behavior, etc.)

- **v2 foundations (`/debug/*`) are evolving.** Iteration is expected, but please:
  - Be explicit in the PR about behavior changes and tradeoffs
  - Add tests for anything that could regress core semantics (replay, DAG scheduling, timers, artifacts)


## Tests

### Unit tests
```bash
npm ci --prefix ui
npm run build --prefix ui
go test ./... -count=1
```

### Integration tests (requires `integration` build tag)
```bash
go test -tags=integration ./... -count=1
```

Recommended “shake out flakes” runs:
```bash
go test -tags=integration ./... -race -count=1 -shuffle=on
go test -tags=integration ./... -race -count=5 -shuffle=on
```


## Load testing

There’s a helper script at `scripts/loadtest.sh`. It uses `hey` (see README for details).

```bash
# Start the server first (see README.md for the exact command)
./scripts/loadtest.sh
```

## Coding expectations

- Run `gofmt` (or let your editor do it automatically).
- Keep PRs small and focused (one fix/feature per PR).
- Add or update tests when changing behavior.
- If you change API behavior, update docs under `docs/` too.


## Important compatibility note (WAL)

WAL is **forward-compatible only**. Once you write WAL entries with newer ops, you can’t safely downgrade to an older binary that doesn’t understand them.

So: if your change affects persistence/durability, call that out clearly in the PR description (and in docs if needed).


## Pull request checklist

Before you open a PR:
- [ ] Tests pass locally (unit + relevant integration)
- [ ] Docs updated if behavior/API changed
- [ ] Clear description: what changed + why
- [ ] Link the issue (or explain the motivation)


## Reporting bugs / requesting features

Please open a GitHub Issue and include:
- What you expected vs what happened
- Minimal repro steps (commands / curl / driftqctl)
- Logs (the more concrete the better)
- Your environment (OS, Go version, commit/tag)
