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




## [0.3.0] - 2026-02-16

### Added
- Record detail relationship diagram (Mermaid)
  - Contextual C1 → C2 → C3 → C4 tree for the current record
  - Current node and related branches highlighted
  - Clickable nodes linking to the corresponding record detail
  - Scrollable container to support large graphs

- Dynamic dropdowns driven by LOOKUPS
  - Lookup-defined fields render as select dropdowns
  - Supports single-value and multi-value lookups
  - Consistent validation between UI and rules engine

- Dynamic dropdowns driven by parent values
  - `c#_human_id` (parent-related) fields render using available parent values
  - Consistent validation between UI and available values

- Schema template management and schema version control
  - Versioned `registry_template.xlsx` as base template
  - New META keys:
    - `schema_version`
    - `schema_hash`
    - `schema_dirty` (registry)
  - Automatic per-sheet column diff (template vs registry)
  - Persistent banner when schema differs or template is missing
  - New Home panel to:
    - Create registry from template
    - Promote new registry fields into template
    - Migrate registry by adding missing template columns
  - When adding a field via UI:
    - registry marked as `schema_dirty`
    - `schema_hash` updated
  - Template ↔ registry sync is explicit (never automatic)

- C4 (RUN) report export in DOCX and HTML
  - New endpoints:
    - `GET /report/c4/{human_id}.docx` (docxtpl)
    - `GET /report/c4/{human_id}.html`
    - `GET /report/c4/{human_id}.html?raw=1` (copy/paste friendly)
  - Reuses existing diagram generation for consistency
  - Mermaid diagram rendering to PNG via mermaid-cli when available
  - Graceful fallback when PNG rendering is unavailable (HTML remains usable)
  - Templates:
    - `c4_chain_report.docx`
    - `c4_chain_report.html.j2`
  - UI buttons in C4 record view to generate Word/HTML reports
  - Report content (same in both formats):
    - Scope section, Mermaid diagram, chain details (C1 → C4)
    - C3 siblings list, C4 siblings list
    - Issues grouped by level (C1..C4) for the main chain only
  - New `report_service` shared module
  - Optional `SAR_MMDC_PATH` to define Mermaid CLI path

- Authentication & RBAC (Phase 1 hardened)
  - Login page (`/login`) and logout endpoint (`/logout`)
  - Signed, time-limited session cookies (itsdangerous)
  - Argon2 password hashing
  - Role-based access control (viewer / editor / admin)
  - YAML user store: `data/users.yml`
  - Middleware injects authenticated user into request state
  - `scripts/create_user.py` for secure user creation
  - Windows helpers: `create_user.bat`, `run_sar.bat`
  - Hardened `users.yml` path resolution anchored to project root
  - All endpoints require auth except `/login`
  - Mutating endpoints protected by role checks
  - Templates show current user and logout button
  - App requires `SECRET_KEY` for session signing

### Changed
- Project refactor to standard `src/` layout
  - Source moved under `src/sar/`
  - Installable as a Python package (`pip install -e .`)
  - Imports no longer depend on the execution directory

- Engine validations made fully dynamic and drift-aware
  - Removed hardcoded lookup-field mappings from `engine.py`
  - Dynamic lookup validation:
    - Applies all `lookup_name` entries in LOOKUPS automatically
    - Validation is driven by `lookup_name == column_name`
    - Applies per level (C1–C4) and ALL
  - Dynamic RULES validation:
    - Rules applied only if referenced field exists
    - Missing rule fields generate `config_missing_field` issues instead of failing
  - Configuration drift detection:
    - LOOKUPS referencing missing fields raise an ISSUE
    - RULES referencing missing fields raise an ISSUE
  - Core validations remain prioritized and always enforced:
    - Required structural fields (human_id, status, name, parent refs)
    - Hierarchical integrity
    - Derived logic (e.g., vulnerabilities_detected)

### Security
- Passwords are no longer stored in plaintext
- Sessions are signed and time-limited
- Server-side role checks enforced for mutating endpoints
