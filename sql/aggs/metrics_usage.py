from prometheus_client import Counter
from toolbox.sql.aggs import Metric
from wrapt import decorator
from collections.abc import Awaitable, Iterable, Mapping, Callable


__metrics_usage = None


def __ensure_metrics_usage(
    metrics: Callable[[], Iterable[Metric]],
    subsystem: str,
):
    global __metrics_usage
    if __metrics_usage is None:

        def normilize(name: str) -> str:
            return ''.join(filter(str.isalnum, name))

        def counter(metric: Metric) -> Counter:
            name = metric.get_display_name()
            return Counter(
                name=normilize(name),
                documentation=f'represents {name} metric usage.',
                subsystem=subsystem,
            )

        __metrics_usage = {m.name: counter(m) for m in metrics()}


def track_metric_usage(
    metrics: Callable[[], Iterable[Metric]],
    subsystem: str,
):
    __ensure_metrics_usage(metrics, subsystem)

    @decorator
    async def usage(
        func: Awaitable,
        instance,
        args,
        kwargs: Mapping,
    ) -> Awaitable:
        metric = kwargs.get('metric')
        usage = __metrics_usage.get(metric)
        if usage:
            usage.inc()
        return await func(*args, **kwargs)

    return usage
