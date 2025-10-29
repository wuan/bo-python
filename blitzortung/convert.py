def value_to_string(value):
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)
