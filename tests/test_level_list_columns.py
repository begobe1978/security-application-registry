import pandas as pd

from sar.core.utils import level_list_columns


def test_level_list_columns_keep_only_id_and_parent_as_reserved_columns():
    df = pd.DataFrame(
        columns=["id", "name", "status", "owner", "c2_id", "description", "repo"]
    )

    cols = level_list_columns(df, id_col="id", parent_col="c2_id", limit=5, show_all=False)

    assert cols == ["id", "c2_id", "name", "status", "owner"]


def test_level_list_columns_follow_workbook_order_for_remaining_columns():
    df = pd.DataFrame(columns=["id", "owner", "description", "status", "c1_id", "name"])

    cols = level_list_columns(df, id_col="id", parent_col="c1_id", limit=4, show_all=False)

    assert cols == ["id", "c1_id", "owner", "description"]


def test_level_list_columns_full_mode_returns_all_columns_except_internal():
    df = pd.DataFrame(columns=["id", "c3_id", "name", "__orphan", "status"])

    cols = level_list_columns(df, id_col="id", parent_col="c3_id", limit=3, show_all=True)

    assert cols == ["id", "c3_id", "name", "status"]
