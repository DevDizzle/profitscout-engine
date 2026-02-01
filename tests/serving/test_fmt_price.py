from src.serving.core.pipelines.page_generator import _fmt_price


def test_fmt_price_formats():
    # Current behavior (g format):
    # 82.5 -> 82.5
    # 82.0 -> 82

    # Desired behavior:
    # 82.5 -> 82.50
    # 82.0 -> 82 (presumed, based on existing docstring "remove trailing zero decimal if integer")
    # 82.55 -> 82.55

    assert _fmt_price(82.5) == "82.50"
    assert _fmt_price(82.50) == "82.50"
    assert _fmt_price(82.0) == "82"
    assert _fmt_price(82) == "82"
    assert _fmt_price(0.5) == "0.50"
    assert _fmt_price("82.5") == "82.50"
