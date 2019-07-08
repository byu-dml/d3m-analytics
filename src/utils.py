def has_path(dictionary: dict, keys: list) -> bool:
    """
    Checks to see if `dictionary` has the path that exists
    along `keys`.
    """
    ptr = dictionary
    for key in keys:
        if key in ptr:
            ptr = ptr[key]
        else:
            return False
    # We've made it through all the keys. The whole path exists.
    return True


def enforce_field(should_enforce_field: bool, d: dict, field_name: str):
    if should_enforce_field and field_name not in d:
        raise ValueError(f"document must have a(n) {field_name}")


def file_len(fname):
    """
    Returns the number of lines in a file.
    Source: https://stackoverflow.com/questions/845058/how-to-get-line-count-cheaply-in-python
    """
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def set_default(d: dict, key, default):
    """
    Assigns `default` to `d[key]` if dictionary `d` doesn't have
    that key yet.
    """
    if key not in d:
        d[key] = default
