from typing import Any, Optional


def resolve(
    param_name: str,
    operator_value: Optional[Any],
    conn_extra: dict,
    default: Any,
) -> Any:
    # приоритет: параметр оператора → Extra connection → дефолт в коде
    if operator_value is not None:
        return operator_value
    if param_name in conn_extra:
        return conn_extra[param_name]
    return default
