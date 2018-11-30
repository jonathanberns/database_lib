#Standard Library Import
import warnings
warnings.simplefilter('once', UserWarning)

#Sqlite Import
import sqlite3

#Postgres Imports
try:    
    import psycopg2
    import psycopg2.extras
    from psycopg2.extras import RealDictCursor
except ImportError:
    warnings.warn('''
                    In order to use Postgres Database.
                    PIP install psycopg2-binary is required.
                  ''', stacklevel=2)

#MySQL Imports
try:
    import mysql.connector
except ImportError:
    warnings.warn('''
                    In order to use MySQL Database.
                    PIP install mysql-connector is required.
                  ''', stacklevel=2)

####################################################################

class Database(object):

    class Postgres(object):
        def __init__(self):
            self.postgres_conn = None
            self.postgres_cursor = None
            self.postgres_query = None
            self.postgres_results = None
            self.postgres_rowcount = None
            self.postgres_params = None
            
        def postgres_connect(self, connection_dict = {}):
            if len(connection_dict) == 0:
                raise Exception('No database connection information is needed!')

            if not 'password' in connection_dict:
                raise Exception('password is needed!')
            
            if not 'user' in connection_dict:
                raise Exception('user is needed!')

            if not 'host' in connection_dict:
                raise Exception('host is needed!')

            if not 'database' in connection_dict:
                raise Exception('database is needed!')

            try:
                self.postgres_conn = psycopg2.connect(**connection_dict)
            except psycopg2.OperationalError as e:    
                print (e)
            finally:
                return self

        def run_postgres_query(self, query = None, params = []):
            if query is None:
                raise Exception("Query cannot be blank")

            is_error = False
            error_msg = None

            try:
                self.postgres_query = query
                self.postgres_params = params 

                if 'select' in self.postgres_query.lower():
                    self.postgres_cursor = self.postgres_conn.cursor(cursor_factory=RealDictCursor)
                else:
                    self.postgres_cursor = self.postgres_conn.cursor()

                self.postgres_cursor.execute(query, params)
            except psycopg2.ProgrammingError as postgres_error:
                error_msg = postgres_error
                is_error = True
                raise Exception("Database Error: {0}".format(postgres_error))
            finally:
                if is_error:
                    raise Exception("Database Error: {0}".format(error_msg))
                return self

        def get_postgres_count(self):
            try:
                self.postgres_rowcount = self.postgres_cursor.rowcount
            except (Exception, psycopg2.DatabaseError) as postgres_error:    
                raise Exception("Database Error: {0}".format(postgres_error))
            except Exception as postgres_error:
                raise Exception("Database Error: {0}".format(postgres_error))
            finally:
                return self.postgres_rowcount

        def postgres_commit(self):
            try:
                self.postgres_conn.commit()
            except (Exception, psycopg2.DatabaseError) as postgres_error:    
                raise Exception("Database Error: {0}".format(postgres_error))
            except Exception as postgres_error:
                raise Exception("Database Error: {0}".format(postgres_error))
            finally:
                return self

        def postgres_rollback(self):
            try:
                self.postgres_conn.rollback()
            except (Exception, psycopg2.DatabaseError) as postgres_error:    
                raise Exception("Database Error: {0}".format(postgres_error))
            finally:
                return self

        def get_postgres_results(self):
            try:
                self.postgres_results = self.postgres_cursor.fetchall()
            except (Exception, psycopg2.DatabaseError) as postgres_error:    
                raise Exception("Database Error: {0}".format(postgres_error))
            finally:
                return self.postgres_results

        def postgres_close(self):
            try:
                self.postgres_conn.close()
            except (Exception, psycopg2.DatabaseError) as postgres_error:    
                raise Exception("Database Error: {0}".format(postgres_error))

    class Sqlite(object):

        def __init__(self):
            self.sqlite_db = None
            self.sqlite_cursor = None
            self.sqlite_query = None
            self.sqlite_results = None
            self.sqlite_rowcount = None
            self.sqlite_params = None
                    
        def sqlite_connect(self, dbname = None):
            if dbname is None:
                raise Exception('No database has been selected!')
                
            self.sqlite_db = sqlite3.connect(dbname)
            self.sqlite_db.row_factory = self.dict_factory
            self.sqlite_cursor = self.sqlite_db.cursor()
            return self
            
        def dict_factory(self, cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d
                    
        def run_sqlite_query(self, query = None, params = []):

            if query is None:
                raise Exception("Query cannot be blank")
            
            self.sqlite_query = query
            self.sqlite_params = params
            
            try:
                self.sqlite_cursor.execute(self.sqlite_query, self.sqlite_params)
            except Exception as sqlite_error:
                self.sqlite_db.rollback()
                raise Exception("Database Error: {0}".format(sqlite_error))
            except Exception as sqlite_error:
                raise Exception("Database Error: {0}".format(sqlite_error))
            return self

        def sqlite_commit(self):
            try:
                self.sqlite_db.commit()
            except sqlite3.Error as sqlite_error:
                raise Exception("Database Error: {0}".format(sqlite_error))
            except Exception as sqlite_error:
                raise Exception("Database Error: {0}".format(sqlite_error))
            finally:
                return self

        def sqlite_rollback(self):
            try:
                self.sqlite_db.rollback()
            except sqlite3.Error as sqlite_error:
                raise Exception("Database Error: {0}".format(sqlite_error))
            except Exception as sqlite_error:
                raise Exception("Database Error: {0}".format(sqlite_error))
            finally:
                return self
            
        def get_sqlite_results(self):
            try:
                self.sqlite_results = self.sqlite_cursor.fetchall()
            except sqlite3.Error as sqlite_error:
                raise Exception("Database Error: {0}".format(sqlite_error))
            except Exception as sqlite_error:
                raise Exception("Database Error: {0}".format(sqlite_error))
            finally:
                return self.sqlite_results
            
        def get_sqlite_count(self):
            try:
                self.sqlite_rowcount = self.sqlite_cursor.rowcount
            except sqlite3.Error as sqlite_error:
                raise Exception("Database Error: {0}".format(sqlite_error))
            except Exception as sqlite_error:
                raise Exception("Database Error: {0}".format(sqlite_error))
            finally:
                return self.sqlite_rowcount

        def get_sqlite_insert_id(self):
            try:
                return self.sqlite_cursor.lastrowid
            except sqlite3.Error as sqlite_error:
                raise Exception("Database Error: {0}".format(sqlite_error))
            except Exception as sqlite_error:
                raise Exception("Database Error: {0}".format(sqlite_error))
            
        def sqlite_close(self):
            try:
                self.sqlite_db.close()
            except sqlite3.Error as sqlite_error:
                raise Exception("Database Error: {0}".format(sqlite_error))
            except Exception as sqlite_error:
                raise Exception("Database Error: {0}".format(sqlite_error))

        
