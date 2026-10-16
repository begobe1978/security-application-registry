from sar.engine import compute


def test_compute_runs_and_returns_frames(tmp_registry):
    view_full, issues, views_by_level = compute(str(tmp_registry))
    assert view_full is not None
    assert issues is not None
    assert isinstance(views_by_level, dict)
    # We expect at least C1..C4 views present
    for level in ("C1", "C2", "C3", "C4"):
        assert level in views_by_level
        assert not views_by_level[level].empty
