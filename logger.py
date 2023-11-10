import os


def debug(msg: str, *args, **kwargs):
    if int(os.environ.get('DEBUG', 0)):
        print(msg)
