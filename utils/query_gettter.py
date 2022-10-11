from typing import List, Type
from pathlib import Path

from toolbox.sql.sql_query import SqlQuery


class QueryLoader:

    @staticmethod
    def get_queries_in_dir(
        self,
        sql_query_type: Type[SqlQuery],
        dir_path: str,
    ) -> List[SqlQuery]:
        return [
            sql_query_type(query_file_path=path, format_params={})
            for path in Path(dir_path).iterdir() if path.is_file()
        ]
