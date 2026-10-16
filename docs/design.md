# SAR Design Principles

SAR is built around a small set of explicit design decisions.

## 1. Excel is the source of truth

The Excel workbook is the authoritative data source for the inventory.

SAR reads the workbook, validates it, writes changes back to it, and regenerates derived views. It does not introduce any database or secondary persistent store.

## 2. SAR is an interaction layer around the workbook

SAR provides:

- visualization
- editing
- validation
- reporting

around the workbook.

The workbook remains the registry itself.

## 3. Fixed structure, dynamic semantics

SAR keeps a fixed structural skeleton:

- four levels (`C1`..`C4`)
- parent-child relationships between levels
- structural fields (`id`, `status`, `name`, and parent references)
- structural prefixes (`C1-`, `C2-`, `C3-`, `C4-`)

At the same time, the workbook defines a large part of the model dynamically:

- additional fields
- lookup-backed validations
- rule-driven validations
- labels and semantic meaning for each level

This keeps the hierarchy stable while allowing the workbook to adapt its business meaning.

## 4. Structural identifiers are neutral

SAR uses neutral structural identifiers:

- `id`
- `c1_id`
- `c2_id`
- `c3_id`

and prefixes:

- `C1-`
- `C2-`
- `C3-`
- `C4-`

This avoids embedding business semantics such as “project”, “application”, “component”, or “runtime” in the identifier format itself.

## 5. Validation is centralized in the engine and schema helpers

Validation in SAR follows two layers:

### Structural validation

Always enforced by SAR itself:

- required structural fields by level
- unique identifiers within a level
- parent-child consistency
- fixed hierarchy consistency

### Workbook-driven validation

Declared inside the workbook:

- `LOOKUPS`
- `RULES`

This allows the workbook to evolve while keeping core invariants stable.

## 6. Recompute-based consistency

After each write operation, SAR recomputes:

- derived views
- issues
- inherited values

This favors consistency and predictability over incremental update complexity.

## 7. Architectural boundaries

| Concern | Responsibility |
|---|---|
| Inventory storage | Excel workbook |
| Structural model | SAR engine |
| Dynamic attributes | Workbook |
| Editing | SAR |
| Validation | SAR engine + workbook rules |
| Reporting | SAR |
| Integration | Not SAR responsibility |

## 8. App structure

The web layer is intentionally split so that `app.py` focuses on application assembly and shared helpers, while route groups are moved into dedicated modules such as `src/sar/web/record_routes.py`.

This keeps routing concerns separated from the rest of the application logic without changing the Excel-first design.
