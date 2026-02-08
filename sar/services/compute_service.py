# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

from typing import Tuple

import pandas as pd

from engine import compute


def regenerate_views(path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run the engine compute to produce view_full and issues dataframes."""
    return compute(path)
