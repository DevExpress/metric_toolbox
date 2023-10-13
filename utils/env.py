import os


__recalc_from_beginning = 'RECALCULATE_FROM_THE_BEGINNING'


def recalculate_from_beginning():
    return __get_env_val(__recalc_from_beginning) == 1


def reset_recalculate_from_beginning():
    __reset_env_val(__recalc_from_beginning)


__recalc_for_last_months = 'RECALCULATE_FOR_LAST_MONTHS'


def recalculate_for_last_n_months() -> int:
    return __get_env_val(__recalc_for_last_months)


def reset_recalculate_for_last_n_months():
    __reset_env_val(__recalc_for_last_months)


__recalc_for_last_days = 'RECALCULATE_FOR_LAST_DAYS'


def recalculate_for_last_n_days() -> int:
    return __get_env_val(__recalc_for_last_days)


def reset_recalculate_for_last_n_days():
    __reset_env_val(__recalc_for_last_days)


def __get_env_val(env: str):
    return int(os.environ.get(env, 0))


def __reset_env_val(env: str):
    os.environ[env] = str(0)