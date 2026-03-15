# CR_PEERS_JP

See @README.md for the human-readable repo overview.

## Core memory files
- @docs/claude/01-project-overview.md
- @docs/claude/02-build-run-config.md
- @docs/claude/03-output-routing-and-logging.md
- @docs/claude/04-rendering-architecture.md
- @docs/claude/05-executive-and-macro-artifacts.md
- @docs/claude/06-normalization-and-peer-groups.md
- @docs/claude/07-local-macro.md
- @docs/claude/08-corp-overlay.md
- @docs/claude/09-troubleshooting.md
- @docs/claude/10-coding-rules.md
- @docs/claude/99-changelog.md

## Global rules
- Preserve import safety.
- Never reintroduce synthetic production reporting data.
- Update tests when logic changes.
- Update docs/change log when architecture changes.
- Prefer workbook-driven integration contracts over cross-module runtime imports.