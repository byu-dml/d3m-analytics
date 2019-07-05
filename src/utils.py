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


def enforce_digest(should_enforce_digest: bool, d: dict):
    if should_enforce_digest and "digest" not in d:
        raise ValueError("document must have a digest")


def file_len(fname):
    """
    Returns the number of lines in a file.
    Source: https://stackoverflow.com/questions/845058/how-to-get-line-count-cheaply-in-python
    """
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1
