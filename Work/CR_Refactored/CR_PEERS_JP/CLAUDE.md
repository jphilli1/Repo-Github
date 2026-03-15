# CR_PEERS_JP

Automated Credit Risk Performance reporting engine for MSPBNA. See `README.md` for the human-readable overview.

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

- **Always update docs/claude/ files** when making architectural changes, adding charts, or changing data pipelines.
- When logic or architecture changes, you MUST append a dated summary of the change to @docs/claude/99-changelog.md.
- If a change invalidates existing workflow rules, immediately update the corresponding topic file in @docs/claude/ to reflect the new reality. Do NOT update .claude/skills dynamically.
- Preserve import safety — no side effects at module level.
- Never reintroduce synthetic production reporting data.
- Never hardcode CERT numbers — always use env var / config pattern.
- Update tests when logic changes.
- Prefer workbook-driven integration contracts over cross-module runtime imports.
- `rendering_mode.py` is the single canonical source for all rendering abstractions.
- Coverage vs Share label rule is non-negotiable (see `06-normalization-and-peer-groups.md`).
- **Issue Logging & The Auditor:** If you discover a bug, math error, or architectural risk while working on an unrelated task, DO NOT FIX IT and do not get distracted. Log the specific file and function at the bottom of `TODO.md` under "Backlog / Identified Risks". Then, immediately print a message in the terminal advising the user to run `/auditor [file_path]` so the human operator can generate a formal execution plan.
