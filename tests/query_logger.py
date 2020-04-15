from sqlalchemy import event
from sqlalchemy.engine import Engine

try:
    import sqlparse
except ImportError:
    sqlparse = None


def format_sql_statement(cursor, statement, parameters):
    """ Format an SqlAlchemy statement into a valid SQL string """
    if isinstance(parameters, dict):
        # This line produces correct but ugly SQL
        sql = cursor.mogrify(statement, parameters)#.decode()

        # Nicely format SQL (takes a lot of CPU)
        if sqlparse is not None:
            return sqlparse.format(sql, reindent=True)
        else:
            return str(sql)
    elif isinstance(parameters, tuple):
        return '\n'.join(format_sql_statement(cursor, statement, p)
                         for p in parameters)
    elif isinstance(parameters, (int, str, float)):
        return statement + str(parameters)  # ugly but true
    else:
        raise TypeError(type(parameters))


class QueryLogger(list):
    """ Log SQL queries """

    def __init__(self, engine: Engine):
        super().__init__()
        self.engine = engine

    # NOTE: this logger subscribes to events dynamically. Never use it in a multi-threaded environment!

    def start_logging(self):
        event.listen(self.engine, "after_cursor_execute", self._after_cursor_execute_event_handler, named=True)

    def stop_logging(self):
        event.remove(self.engine, "after_cursor_execute", self._after_cursor_execute_event_handler)

    def _after_cursor_execute_event_handler(self, conn, cursor, statement, parameters, **kw):
        self.append(
            format_sql_statement(cursor, statement, parameters)
        )

    def print_log(self):
        for i, q in enumerate(self):
            print('=' * 5, ' Query #{}'.format(i))
            print(q)

    # Context manager

    def __enter__(self):
        self.start_logging()
        return self

    def __exit__(self, *exc):
        self.stop_logging()
        if exc != (None, None, None):
            self.print_log()
        return False
