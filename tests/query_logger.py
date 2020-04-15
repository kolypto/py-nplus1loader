import re
import sys

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Query
from sqlalchemy.dialects import postgresql as pg

PY2 = sys.version_info[0] == 2


def _insert_query_params(statement_str, parameters, dialect):
    """ Compile a statement by inserting *unquoted* parameters into the query """
    return statement_str % parameters


def stmt2sql(stmt):
    """ Convert an SqlAlchemy statement into a string """
    # See: http://stackoverflow.com/a/4617623/134904
    # This intentionally does not escape values!
    dialect = pg.dialect()
    query = stmt.compile(dialect=dialect)
    return _insert_query_params(query.string, query.params, pg.dialect())


def q2sql(q):
    """ Convert an SqlAlchemy query to string """
    return stmt2sql(q.statement)


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

    def _after_cursor_execute_event_handler(self, **kw):
        self.append(_insert_query_params(kw['statement'], kw['parameters'], kw['context']))

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
