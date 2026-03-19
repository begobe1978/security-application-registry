# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

# Maximum number of columns shown in level lists when using the limited view.
# Reserved structural columns (`id` and the parent reference column when present)
# always take precedence; the remaining slots are filled following the workbook
# column order.
LIST_VIEW_MAX_COLUMNS = 8
