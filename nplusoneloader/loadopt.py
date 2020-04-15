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
    loadopt = loadopt.nplus1loader_cols(*attrs)
    loadopt = loadopt.nplus1loader_rels(*attrs)
    return loadopt

# Unbound versions of those very loaders
# "Unbound" means that the loader can be imported and used like you always do:
#       options( nplus1loader('*') )
# They are called "Unbound" because they aren't bound to a Model yet.

@default_columns._add_unbound_fn
def default_columns(model):
    return _UnboundLoad().default_columns(model)


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
nplus1loader_cols = nplus1loader_cols._unbound_fn
nplus1loader_rels = nplus1loader_rels._unbound_fn
nplus1loader = nplus1loader._unbound_fn

