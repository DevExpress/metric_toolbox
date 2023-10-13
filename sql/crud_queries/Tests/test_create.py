import pytest
from toolbox.sql.crud_queries.create import (
    SqliteCreateTableQuery,
    SqliteCreateTableFromTableQuery,
    QueryField,
    DropRowsTriggerParams,
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
            '''DROP TABLE IF EXISTS target;
CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	name_target TEXT,
	PRIMARY KEY (
		id_target
	)
) WITHOUT ROWID;
CREATE TRIGGER ...

INSERT INTO target
SELECT DISTINCT 
	id AS id_target,
	name AS name_target
FROM source
WHERE id IS NOT NULL
ON CONFLICT(id_target) DO UPDATE SET
                name_target=excluded.name_target''',
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
                ),
                keep_rows_for_last=DropRowsTriggerParams(
                    modifier='1 YEARS',
                    date_field='qwe',
                ),
            ),
            '',
            '''DROP TABLE IF EXISTS target;
CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	name_target TEXT,
	PRIMARY KEY (
		id_target
	)
) WITHOUT ROWID;

CREATE TRIGGER IF NOT EXISTS trgrtarget AFTER INSERT ON target
BEGIN
    DELETE FROM target WHERE qwe < (SELECT DATE(MAX(qwe), '-1 YEARS') from target);
END;

INSERT INTO target
SELECT DISTINCT 
	id AS id_target,
	name AS name_target
FROM source
WHERE id IS NOT NULL
ON CONFLICT(id_target) DO UPDATE SET
                name_target=excluded.name_target''',
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
            '''DROP TABLE IF EXISTS target;
CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	uid_target INTEGER,
	name_target TEXT,
	PRIMARY KEY (
		id_target,
		uid_target
	)
) WITHOUT ROWID;
''',
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
                keep_rows_for_last=DropRowsTriggerParams(
                    modifier='1 YEARS',
                    date_field='qwe',
                ),
            ),
            '',
            '''DROP TABLE IF EXISTS target;
CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	uid_target INTEGER,
	name_target TEXT,
	PRIMARY KEY (
		id_target,
		uid_target
	)
) WITHOUT ROWID;

CREATE TRIGGER IF NOT EXISTS trgrtarget AFTER INSERT ON target
BEGIN
    DELETE FROM target WHERE qwe < (SELECT DATE(MAX(qwe), '-1 YEARS') from target);
END;
''',
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
            '''CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	uid_target INTEGER,
	name_target TEXT,
	PRIMARY KEY (
		id_target,
		uid_target
	)
) WITHOUT ROWID;
''',
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
            '''CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	uid_target INTEGER,
	PRIMARY KEY (
		id_target,
		uid_target
	)
) WITHOUT ROWID;
''',
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
            '''CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	uid_target INTEGER
);
''',
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
            '''CREATE TABLE IF NOT EXISTS target (
	id_target TEXT,
	uid_target INTEGER
);
CREATE TRIGGER ...''',
        ),
    )
)
def test_get_script(
    query: CRUDQuery,
    extender: str,
    res: str,
):
    assert query.get_script(extender) == res
