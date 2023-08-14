import os


def recalculate_from_beginning():
    return int(os.environ['RECALCULATE_FROM_THE_BEGINNING']) == 1


def reset_recalculate_from_beginning():
    os.environ['RECALCULATE_FROM_THE_BEGINNING'] = str(0)
