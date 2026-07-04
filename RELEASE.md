# Release checklist (Tendwire source-only RC)

Build release artifacts from a **clean git checkout only**. Never zip the working
directory directly — it can contain `__pycache__/`, `*.pyc`, `.pytest_cache/`,
and local `*.db` state that must not ship. `.gitignore` already excludes these,
so building from git is what guarantees a clean artifact.

## 1. Preconditions

```sh
git status --porcelain            # must be empty
python -m py_compile $(find src tests -name '*.py')
python -m pytest -q               # all green
```

## 2. Build a clean artifact

Source zip/tar (tracked files only, respects `.gitignore`):

```sh
git archive --format=zip -o dist/tendwire-$(git describe --always).zip HEAD
```

Or a Python sdist/wheel (hatchling; packages `src/tendwire` + declared includes):

```sh
python -m build
```

## 3. Verify the artifact is clean

The following must print nothing:

```sh
git archive --format=tar HEAD | tar -t | grep -E '__pycache__|\.pyc$|\.pytest_cache|\.db$'
```

For sdist/wheel:

```sh
tar -tf dist/*.tar.gz | grep -E '__pycache__|\.pyc$|\.pytest_cache' || echo clean
```

## 4. Local hygiene (optional, before building from a dirty tree)

```sh
find . -type d -name __pycache__ -not -path './.git/*' -exec rm -rf {} +
rm -rf .pytest_cache
find . -name '*.pyc' -not -path './.git/*' -delete
```

## Notes

- `HANDOFF.md` and `*.db` are git-ignored and never appear in `git archive`.
- The public contract shipped is `command.submit` (`tendwire command --json`);
  see the README "Send transport" section. No `pane_id`/`send_keys` is exposed.
- `tests/test_release_readiness.py` guards the public JSON contract (zero
  forbidden keys, no pseudo pane ids).
