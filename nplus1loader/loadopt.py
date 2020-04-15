""" Load options """
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm.strategy_options import loader_option, _UnboundLoad

from . import strategies  # make sure it's registered


# Load interface: register some new loading options
# This means that when you do Load(Model), you can use those functions as methods:
# For instance:
#       Load(Model).default_columns()

@loader_option()
def default_columns(loadopt: _UnboundLoad, model: type):
    """ A loader option to read the default column strategies from a Model.

    Usage:

        ssn.query(User).options(
            # Read the default deferred/undeferred column preferences from a model
            default_columns(User),
            # Defer one additional column
            defer('login')
        )
    """
    mapper = class_mapper(model)

    # Go through every column and get its strategy, adding it to this Load()
    for col in mapper.column_attrs:
        loadopt = loadopt.set_column_strategy(
            # Example: 'password_hash'
            (col.key,),
            # Example: (('deferred', True), ('instrument', True))
            dict(col.strategy_key)
        )
    return loadopt


@loader_option()
def raiseload_col(loadopt: _UnboundLoad, *attrs):
    """ Raise an exception when a column is lazy-loaded

    Note that if you use it as a catch-all raiseload_col('*") option, it will also defer the primary key.
    Use default_columns() to avert that.
    """
    loadopt = loadopt.set_column_strategy(
        attrs, {"raiseload_col": True}
    )
    return loadopt


@loader_option()
def raiseload_rel(loadopt: _UnboundLoad, *attrs):
    """ Raise an exception when a relationship is lazy-loaded """
    for attr in attrs:
        loadopt = loadopt.set_relationship_strategy(
            attr, {"lazy": "raiseload_rel"}
        )
    return loadopt


@loader_option()
def raiseload_all(loadopt: _UnboundLoad, *attrs):
    """ Raise an exception when a column or a relationship is lazy-loaded

    In an ideal world, you'd do something like this:

        ssn.query(User).options(
            default_columns(User),
            # Raise exceptions in unit-tests;
            # Do N+1 loading in production
            raiseload_all('*') if IN_TESTING else nplus1loader('*')
        )
    """
    assert tuple(attrs) == ('*',), 'raiseload_all() only supports "*" yet'
    loadopt = loadopt.raiseload_col('*')  # columns
    loadopt = loadopt.raiseload_rel('*')  # relationships
    return loadopt


@loader_option()
def nplus1loader_cols(loadopt: _UnboundLoad, *attrs):
    """ N+1 loader for columns

    Give it a list of columns, of '*' to handle them all.
    """
    loadopt = loadopt.set_column_strategy(
        attrs, {"nplus1": True}
    )
    return loadopt


@loader_option()
def nplus1loader_rels(loadopt: _UnboundLoad, *attrs):
    """ N+1 loader for relationships

    Give it a list of relationships, of '*' to handle them all.
    """
    for attr in attrs:
        loadopt = loadopt.set_relationship_strategy(
            attr, {"lazy": "nplus1"}
        )
    return loadopt


@loader_option()
def nplus1loader(loadopt, attrs):
    """ N+1 loader for attributes, be it a column or a relationship

    Give it a list of attributes, of '*' to handle them all.
    """
    assert tuple(attrs) == ('*',), 'nplus1loader() only supports "*" yet'
    loadopt = loadopt.nplus1loader_cols('*')
    loadopt = loadopt.nplus1loader_rels('*')
    return loadopt


# Unbound versions of those very loaders
# "Unbound" means that the loader can be imported and used like you always do:
#       options( nplus1loader('*') )
# They are called "Unbound" because they aren't bound to a Model yet.

@default_columns._add_unbound_fn
def default_columns(model):
    return _UnboundLoad().default_columns(model)


@raiseload_col._add_unbound_fn
def raiseload_col(*attrs):
    return _UnboundLoad().raiseload_col(*attrs)


@raiseload_col._add_unbound_fn
def raiseload_rel(*attrs):
    return _UnboundLoad().raiseload_rel(*attrs)


@raiseload_all._add_unbound_fn
def raiseload_all(*attrs):
    return _UnboundLoad().raiseload_all(*attrs)


@nplus1loader_cols._add_unbound_fn
def nplus1loader_cols(*attrs):
    return _UnboundLoad().nplus1loader_cols(*attrs)


@nplus1loader_rels._add_unbound_fn
def nplus1loader_rels(*attrs):
    return _UnboundLoad().nplus1loader_rels(*attrs)


@nplus1loader._add_unbound_fn
def nplus1loader(*attrs):
    return _UnboundLoad().nplus1loader(*attrs)


# The unbound loader options that you're going to import and use

default_columns = default_columns._unbound_fn

raiseload_col = raiseload_col._unbound_fn
raiseload_rel = raiseload_rel._unbound_fn
raiseload_all = raiseload_all._unbound_fn

nplus1loader_cols = nplus1loader_cols._unbound_fn
nplus1loader_rels = nplus1loader_rels._unbound_fn
nplus1loader = nplus1loader._unbound_fn

