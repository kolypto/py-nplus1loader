from typing import Tuple, Union
from sqlalchemy.orm.interfaces import StrategizedProperty
from sqlalchemy.orm import RelationshipProperty, Query
from sqlalchemy.orm.strategy_options import Load


def query_nplus1loader_others(query: Query) -> Query:
    """ Go through all related entities in the Query and apply nplus1loader() to them

    The problem:
    If you just put an nplus1loader('*') on a Query, it will only apply to top-level entities:
    meaning, those columns and relationships that are on the immediate model you're loading.
    It won't apply to relationships that were manually specified as joinedload()
    unless you put an additional nplus1loader('*') on that relationship!

    The solution:
    This function goes through all relationships specified on the query and manually puts
    a terminating catch-all nplus1loader('*') on every single one of them.
    """
    # Step #1: find all related entities on this Query.
    # Step #2: put an nplus1loader('*') on them
    #
    # To find all related entities, we iterate the Query._attributes mapping
    # which gives loader options for every attribute.
    #
    # Now this Query._attributes is weird.
    # It has one special key, "_unbound_load_dedupes", which we ignore (contains unbound loader options)
    #
    # All other keys are in the form of tuples: ('loader', loader_path),
    # where `loader_path` is a tuple of unwound (Mapper, Property) pairs:
    #   ('loader', (Mapper(), ColumnProperty())) -> Load()
    #   ('loader', (Mapper(), RelationshipProperty())) -> Load()
    #   ('loader', (Mapper(), RelationshipProperty(), Mapper(), ColumnProperty())) -> Load()
    #   ('loader', (Mapper(), RelationshipProperty(), Mapper(), 'columns:*')) -> Load()
    #
    # We will iterate them and try to find loader paths ending with `RelationshipProperty()`,
    # and use their `Load()` interface to add one more option: nplus1loader()
    loader: Load
    for key, loader in list(query._attributes.items()):
        # only look into those ('loader', path) tuples
        # also make sure that `path` is not empty (or else we don't care)
        if isinstance(key, tuple) and key[0] == 'loader' and len(key[1]):
            loader_path: Tuple[Union[StrategizedProperty, str]] = key[1]
            target_col = loader_path[-1]
            # we're only interested in paths ending with a `RelationshipProperty`
            if isinstance(target_col, RelationshipProperty):
                # Put a default_columns().npus1loader() onto it
                loader.default_columns(target_col.mapper.class_).nplus1loader('*')
    return query
