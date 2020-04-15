""" Bulk load an attribute for multiple instances at once """

from typing import Tuple, Iterable

from funcy import chunks
from sqlalchemy import Column, tuple_
from sqlalchemy.orm.attributes import set_committed_value
from sqlalchemy.orm.util import identity_key
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.orm.state import InstanceState
from sqlalchemy.orm import Mapper, Session, Query, defaultload, joinedload


# These functions implement bulk lazy loading
# In other words: given a list of instances, it can load one particular attribute's value on all of them.

def bulk_load_attribute_for_instance_states(session: Session, mapper: Mapper, states: Iterable[InstanceState], attr_name: str):
    """ Given a list of partially-loaded instances, augment them with an attribute `attr_name` by loading it from the DB

    It will augment all instances in chunks, not all at once.

    Args:
        session: The Session to use for loading
        mapper: The Mapper all those instances are handled with
        states: The instances to augment
        attr_name: The attribute to load
    """
    # Are we dealing with a column, or with a relationship?
    if attr_name in mapper.columns:
        loader_func = _bulk_load_column_for_instance_states
    elif attr_name in mapper.relationships:
        loader_func = _bulk_load_relationship_for_instance_states
    else:
        # Neither a column nor a relationship. What is it?
        raise KeyError(attr_name)

    # We're going to make SQL queries, so we have to temporarily disable Session's autoflush.
    # If we don't, it may try to save any unsaved instances.
    with session.no_autoflush:
        # Iterate those instances in bite-size chunks
        # `500` is the number SqlAlchemy uses internally with SelectInLoader
        for states_chunk in chunks(500, states):
            # First, collect primary keys from those incomplete instances
            identities = (state.identity for state in states_chunk)

            # Now, augment those instances by loading the missing attribute `attr_name` from the database
            loader_func(session, mapper, identities, attr_name)


def _bulk_load_column_for_instance_states(session: Session, mapper: Mapper, identities: Iterable[Tuple], attr_name: str):
    """ Load a column attribute for a list of instance states where the attribute is unloaded """
    Model = mapper.class_
    attr: Column = mapper.columns[attr_name]

    # Using those identities (primary keys), load the missing attribute
    q = load_by_primary_keys(session, mapper, identities, attr)

    # Having the missing attribute's value loaded, assign it to every instance in the session
    for identity, attr_value in q:
        # Build the identity key the way SqlAlchemy likes it:
        # (Model, primary-key, None)
        key = identity_key(Model, identity)

        # We do not iterate the Session to find an instance that matches the primary key.
        # Instead, we take it directly using the `identity_map`
        instance = session.identity_map[key]

        # Set the value of the missing attribute.
        # This is how it immediately becomes loaded.
        # Note that this action does not overwrite any modifications made to the attribute.
        set_committed_value(instance, attr_name, attr_value)


def _bulk_load_relationship_for_instance_states(session: Session, mapper: Mapper, identities: Iterable[Tuple], attr_name: str):
    """ Load a relationship attribute for a list of instance states where the attribute is unloaded """
    Model = mapper.class_
    relationship: Column = mapper.all_orm_descriptors[attr_name]

    # Prepare the primary key
    pk_columns = get_primary_key_columns(mapper)
    pk_column_names = [col.key for col in mapper.primary_key]

    # Using those identities (primary keys), load the missing attribute from the DB and put it into instances
    #
    # Note that we won't do anything manually here. We just make a query, and seemingly throw it away.
    # But what happens here is that we have a model that's partially loaded:
    #       defaultload(Model).load_only(primary key fields)
    # This tells SqlAlchemy that the query contains `Model` instances.
    # Then we load a relationship using joinedload().
    #
    # Because all those instances are already in SqlAlchemy's Session which maintains an identity map,
    # when those additional relationships are loaded from the database... they will automatically augment
    # the instances that are already in the session.
    #
    # So here we rely on the fact that as soon as other, yet unloaded, fields become available,
    # SqlAlchemy adds them to existing instances (!)
    #
    # Magic.
    session.query(Model).options(
        defaultload(Model).load_only(*pk_column_names),
        joinedload(relationship)
    ).filter(
        build_primary_key_condition(pk_columns, identities)
    ).all()


def load_by_primary_keys(session: Session, mapper: Mapper, identities: Iterable[Tuple], *entities) -> Query:
    """ Given a Session, load many instances using a list of their primary keys

    Args:
        session: The Session to use for loading
        mapper: The mapper to filter the primary keys from
        identities: An itarable of identities (primary key tuples)
        entities: Additional entities to load with ssn.query(...)

    Returns:
        A Query.
        First field "pk": the identity tuple (the primary key)
        Other fields: the *entities you wanted loaded
    """
    pk_columns = get_primary_key_columns(mapper)

    # Load many instances by their primary keys
    #
    # First of all, we need to load the primary key, as well as the missing column's value, so it looks like we need
    #       pk_col1, pk_col2, ..., attr_value
    # But then in Python we would have to slice the list.
    # But because Postgres supports tuples, we select a tuple of the primary key instead:
    #       (pk_col1, pk_col2, ...), attr_value
    # Just two columns, one being a composite primary key.
    # It perfectly matches SqlAlchemy's instance identity, which is a tuple of primary keys.
    #
    # Secondly, the primary key condition. We're going to load N intances by their primary keys.
    # We could have done like this:
    #       WHERE (pk_col1=:val AND pk_col2=:val) OR (pk_col1=:val AND pk_col2=:val) OR ...
    # but once again, tuples are very convenient and readable:
    #       WHERE (pk_col1, pk_col2) IN ((:val, :val), (:val, :val), ...)
    #
    # Thanks for this neat trick, @vdmit11 :)
    return session.query(
        # That's the primary key tuple
        tuple_(*pk_columns).label('pk'),
        # Additional entities you want to load
        *entities
    ).filter(
        build_primary_key_condition(pk_columns, identities)
    )


def build_primary_key_condition(pk_columns: Tuple[Column], identities: Iterable[Tuple]) -> BinaryExpression:
    """ Build an IN(...) condition for a primary key to select many instances at once

    Args:
        pk_columns: The columns to filter with
        identities: An iterable of identities (primary key values)

    This conditon builder uses tuples:

        tuple(primary-key-columns) = [ tuple(primary-key-values), ... ]

    The resulting query looks like this:

        WHERE (pk_col1, pk_col2) IN ((:val, :val), (:val, :val), ...)
    """
    return tuple_(*pk_columns).in_(identities)


def get_primary_key_columns(mapper: Mapper) -> Tuple[Column]:
    """ Get a tuple of primary key columns for a Mapper

    If you have a Model, use get_mapper(model)
    """
    return mapper.primary_key
