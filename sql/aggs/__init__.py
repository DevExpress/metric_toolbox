from toolbox.sql.aggs.metrics import Metric
from toolbox.sql.aggs.functions import SUM, COUNT, COUNT_DISTINCT, AVG
from toolbox.sql.aggs.groups import GroupBy, Window


NONE_METRIC = Metric('Fake', '', 'Empty', SUM(0))
