from toolbox.sql.aggs.metrics import Metric
from toolbox.sql.aggs.functions import SUM, COUNT, COUNT_DISTINCT


none_metric = Metric('Fake', SUM(0))
