# Home_Projects

## Claude Memory File Layout (CR_PEERS_JP)

The `Work/CR_Refactored/CR_PEERS_JP/` project uses a modular Claude memory layout:

| File | Topic |
|---|---|
| `CLAUDE.md` | Thin index — imports all topic files via `@path` |
| `docs/claude/01-project-overview.md` | Pipeline summary, module table |
| `docs/claude/02-build-run-config.md` | Run commands, env vars, dependencies |
| `docs/claude/03-output-routing-and-logging.md` | Output routing, naming, CSV logging |
| `docs/claude/04-rendering-architecture.md` | Dual-mode rendering, manifest, canonical rules |
| `docs/claude/05-executive-and-macro-artifacts.md` | Executive charts, KRI, macro overlays, FRED expansion |
| `docs/claude/06-normalization-and-peer-groups.md` | Normalization, exclusion stack, peer groups, HUD enrichment |
| `docs/claude/07-local-macro.md` | Geography spine, BEA/BLS/Census APIs, per-capita math |
| `docs/claude/08-corp-overlay.md` | Corp overlay standalone contract |
| `docs/claude/09-troubleshooting.md` | Common errors, known issues |
| `docs/claude/10-coding-rules.md` | No-hardcoding, import safety, chart palette, label policy |
| `docs/claude/99-changelog.md` | All dated change history (read-only reference) |
| `docs/claude/_archive/` | Pre-split monolith archive |

Subtree memory files: `src/reporting/CLAUDE.md`, `src/local_macro/CLAUDE.md`, `tests/CLAUDE.md`.