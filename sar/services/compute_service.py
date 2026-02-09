# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

from typing import Tuple, Dict

import pandas as pd

from engine import compute


def regenerate_views(path: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, pd.DataFrame]]:
    """Run the engine compute to produce:

    - view_full (runtime-centric chain)
    - issues (rules + validations)
    - views_by_level (C1..C4 derived, never raw Excel)
    """
    return compute(path)
