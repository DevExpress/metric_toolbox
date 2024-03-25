import toolbox.config as config


def debug(msg: str, *args, **kwargs):
    if config.debug():
        print(msg)
