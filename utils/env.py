import os


__recalc_from_beginning = 'RECALCULATE_FROM_THE_BEGINNING'


def recalculate_from_beginning():
    return int(os.environ[__recalc_from_beginning]) == 1


def reset_recalculate_from_beginning():
    os.environ[__recalc_from_beginning] = str(0)


__recalc_for_last_months = 'RECALCULATE_FOR_LAST_MONTHS'


def recalculate_for_last_n_months() -> int:
    return int(os.environ.get(__recalc_for_last_months, 0))


def reset_recalculate_for_last_n_months():
    os.environ[__recalc_for_last_months] = str(0)
