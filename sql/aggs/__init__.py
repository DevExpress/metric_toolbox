from toolbox.sql.aggs.metrics import Metric
from toolbox.sql.aggs.functions import SUM, COUNT, COUNT_DISTINCT


NONE_METRIC = Metric('Fake', '', 'Empty', SUM(0))
