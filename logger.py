import os


def debug(msg: str, *args, **kwargs):
    if os.environ.get('DEBUG', None):
        print(msg)
