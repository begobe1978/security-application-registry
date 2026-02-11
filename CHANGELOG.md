## [0.2.0] - 2026-02-11
### Added
- Record detail relationship diagram (Mermaid) with clickable nodes, contextual C1→C4 tree, highlighting and scroll container.
- Dynamic dropdowns from LOOKUPS (single and multi-value) with consistent UI + engine validation.
- Dynamic dropdowns from parent fields (c#_human_id) based on available parent values.

### Changed
- Project restructured to standard `src/` layout; package is installable (e.g. `pip install -e .`) and imports no longer depend on CWD.
- Lookup validation is now fully dynamic: LOOKUPS drives validation automatically by `lookup_name == column_name`, per level (C1–C4) and ALL.
- RULES validation now applies only when referenced fields exist.

### Fixed
- Missing rule fields no longer fail execution; they produce `config_missing_field` issues.
- Added configuration drift detection for LOOKUPS/RULES referencing missing sheet fields.

### Notes
- Core validations remain prioritized: structural required fields, hierarchy integrity, and derived logic.
