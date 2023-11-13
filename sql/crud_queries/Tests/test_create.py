import pytest
from toolbox.sql.crud_queries.create import (
    SqliteCreateTableQuery,
    SqliteCreateTableFromTableQuery,
    QueryField,
)
from toolbox.sql.crud_queries.protocols import CRUDQuery


@pytest.mark.parametrize(
    'query, extender, res', (
        (
            SqliteCreateTableFromTableQuery(
                source_table_or_subquery='source',
                target_table_name='target',
                unique_key_fields=(
                    QueryField(
                        source_name='id',
                        target_name='id_target',
                        type='TEXT',
                    ),
                ),
                values_fields=(
                    QueryField(
                        source_name='name',
                        target_name='name_target',
                        type='TEXT',
                    ),
                )
            ),
            'CREATE TRIGGER ...\n',
            (
'''DROP TABLE IF EXISTS target;
CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	name_target TEXT,
	PRIMARY KEY (
		id_target
	)
) WITHOUT ROWID;
CREATE TRIGGER ...

INSERT INTO target(
	id_target,
	name_target
)
SELECT DISTINCT 
	id AS id_target,
	name AS name_target
FROM source
WHERE id IS NOT NULL'''
            ),
        ),
                (
            SqliteCreateTableFromTableQuery(
                source_table_or_subquery='source',
                target_table_name='target',
                unique_key_fields=(
                    QueryField(
                        source_name='id',
                        target_name='id_target',
                        type='TEXT',
                    ),
                ),
                values_fields=(
                    QueryField(
                        source_name='name',
                        target_name='name_target',
                        type='TEXT',
                    ),
                     QueryField(
                        source_name='uid',
                        target_name='uid_target',
                        type='INTEGER',
                    ),
                ),
                recreate=False,
            ),
            'CREATE TRIGGER ...\n',
            (
'''CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	name_target TEXT,
	uid_target INTEGER,
	PRIMARY KEY (
		id_target
	)
) WITHOUT ROWID;
CREATE TRIGGER ...

INSERT INTO target(
	id_target,
	name_target,
	uid_target
)
SELECT DISTINCT 
	id AS id_target,
	name AS name_target,
	uid AS uid_target
FROM source
WHERE id IS NOT NULL
ON CONFLICT(id_target) DO UPDATE SET
		name_target=excluded.name_target,
		uid_target=excluded.uid_target'''
            ),
        ),
        (
            SqliteCreateTableQuery(
                target_table_name='target',
                unique_key_fields=(
                    QueryField(
                        source_name='id',
                        target_name='id_target',
                        type='TEXT',
                    ),
                    QueryField(
                        source_name='uid',
                        target_name='uid_target',
                        type='INTEGER',
                    ),
                ),
                values_fields=(
                    QueryField(
                        source_name='name',
                        target_name='name_target',
                        type='TEXT',
                    ),
                ),
                recreate=True,
            ),
            '',
            (
'''DROP TABLE IF EXISTS target;
CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	uid_target INTEGER,
	name_target TEXT,
	PRIMARY KEY (
		id_target,
		uid_target
	)
) WITHOUT ROWID;'''
            ),
        ),
        (
            SqliteCreateTableQuery(
                target_table_name='target',
                unique_key_fields=(
                    QueryField(
                        source_name='id',
                        target_name='id_target',
                        type='TEXT',
                    ),
                    QueryField(
                        source_name='uid',
                        target_name='uid_target',
                        type='INTEGER',
                    ),
                ),
                values_fields=(
                    QueryField(
                        source_name='name',
                        target_name='name_target',
                        type='TEXT',
                    ),
                ),
            ),
            '',
            (
'''CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	uid_target INTEGER,
	name_target TEXT,
	PRIMARY KEY (
		id_target,
		uid_target
	)
) WITHOUT ROWID;'''
            ),
        ),
        (
            SqliteCreateTableQuery(
                target_table_name='target',
                unique_key_fields=(
                    QueryField(
                        source_name='id',
                        target_name='id_target',
                        type='TEXT',
                    ),
                    QueryField(
                        source_name='uid',
                        target_name='uid_target',
                        type='INTEGER',
                    ),
                ),
            ),
            '',
            (
'''CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	uid_target INTEGER,
	PRIMARY KEY (
		id_target,
		uid_target
	)
) WITHOUT ROWID;'''
            ),
        ),
        (
            SqliteCreateTableQuery(
                target_table_name='target',
                unique_key_fields=None,
                values_fields=(
                    QueryField(
                        source_name='id',
                        target_name='id_target',
                        type='TEXT',
                    ),
                    QueryField(
                        source_name='uid',
                        target_name='uid_target',
                        type='INTEGER',
                    ),
                ),
            ),
            '',
            (
'''CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	uid_target INTEGER
);'''
            ),
        ),
        (
            SqliteCreateTableQuery(
                target_table_name='target',
                unique_key_fields=None,
                values_fields=(
                    QueryField(
                        source_name='id',
                        target_name='id_target',
                        type='TEXT',
                    ),
                    QueryField(
                        source_name='uid',
                        target_name='uid_target',
                        type='INTEGER',
                    ),
                ),
            ),
            'CREATE TRIGGER ...',
            (
'''CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	uid_target INTEGER
);
CREATE TRIGGER ...'''
            ),
        ),
    )
)
def test_get_script(
    query: CRUDQuery,
    extender: str,
    res: str,
):
    assert query.get_script(extender) == res
