from collections.abc import Iterable


def build_filter_string(filters: Iterable[str]) -> str:
    return '\n\t'.join(filter(None, filters))
