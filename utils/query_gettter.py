from typing import Iterable, Dict, Type
from pathlib import Path

from toolbox.sql.sql_query import SqlQuery


class QueryGetter:

    @staticmethod
    def get_queries_in_dir(
        sql_query_type: Type[SqlQuery],
        format_params: Dict[str, str], 
        dir_path: str,
    ) -> Iterable[SqlQuery]:
        return [
            sql_query_type(query_file_path=path, format_params=format_params)
            for path in Path(dir_path).iterdir() if path.is_file()
        ]
