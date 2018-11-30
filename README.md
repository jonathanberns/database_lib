# database_lib
Python Library connect to multiple databases.

Currently SQLite and PostgresSQL code has been finished.

from database_lib import Database

sqlite_db_obj = Database().Sqlite().sqlite_connect(DATABASE_NAME)

For Select Statements
sqlite_db_obj.run_sqlite_query(SQL_QUERY,[PARAMETERS]).get_sqlite_results()

For all others
sqlite_db_obj.run_sqlite_query(SQL_QUERY,[PARAMETERS])
sqlite_db_obj.get_sqlite_count()
sqlite_db_obj.sqlite_rollback()
sqlite_db_obj.sqlite_commit()
sqlite_db_obj.sqlite_close()

postgres_db_obj = Database().Postgres().postgres_connect({'password': PASSWORD, 'user': USERNAME, 'host': HOSTNAME/IP, 'database': DATABASE_NAME})

For Select Statements
postgres_db_obj.run_postgres_query(SQL_QUERY, [PARAMETERS]).get_postgres_results()

For all others
postgres_db_obj.run_postgres_query(SQL_QUERY, [PARAMETERS])
postgres_db_obj.postgres_commit()
postgres_db_obj.postgres_rollback()
postgres_db_obj.get_postgres_count()
postgres_db_obj.postgres_close()
