from marketing_api.utils.config_resolver import resolve


def test_operator_value_wins_over_extra_and_default():
    assert resolve("timeout", 10, {"timeout": 30}, 60) == 10


def test_extra_used_when_operator_is_none():
    assert resolve("timeout", None, {"timeout": 30}, 60) == 30


def test_default_used_when_missing_everywhere():
    assert resolve("timeout", None, {}, 60) == 60


def test_extra_key_absent_falls_back_to_default():
    assert resolve("max_page_size", None, {"timeout": 30}, 100) == 100


def test_operator_none_with_extra_none_like_value():
    assert resolve("verify_ssl", None, {"verify_ssl": False}, True) is False
