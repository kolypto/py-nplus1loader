from contextlib import contextmanager
from typing import List, ContextManager, Union, Set

import pytest
import sqlalchemy as sa
import sqlalchemy.orm.base
import sqlalchemy.orm.state
from sqlalchemy.orm import defaultload

from nplus1loader import nplus1loader, safeguard_session, default_columns, raiseload_all, LazyLoadingAttributeError
from .conftest import NumbersAndFruits
from .models import Number, Fruit
from .query_logger import QueryLogger


# nplus1 safety methods
METHOD_NONE = None  # none
METHOD_NPLUS1LOADER = nplus1loader  # nplus1loader() option
METHOD_SAFEGUARD = safeguard_session  # session-wide catch-all
LOAD_METHODS = [
    METHOD_NPLUS1LOADER,
    # METHOD_SAFEGUARD,
    METHOD_NONE
]


def test_lazyload_column_without_fixes(ssn: sa.orm.Session, numbers_and_fruits: NumbersAndFruits):
    """ Baseline: column attribute lazy-load triggers many queries """
    numbers = load_numbers(ssn, sa.orm.load_only('id'))

    with query_logger(ssn) as ql:
        # Touch the `.en` on every Number
        touch_attribute('en', *numbers)

        # Result: one query per object
        assert len(ql) == len(numbers)
        assert len(ql) > 3  # sufficiently large to notice


@pytest.mark.parametrize('load_method', LOAD_METHODS)
def test_lazyload_columns(ssn: sa.orm.Session, numbers_and_fruits: NumbersAndFruits, load_method):
    """ Test: lazy-loading columns """
    # Methods
    load_option = DUMMY_LOAD_OPTION
    extra_safeguard_query = 0

    if load_method is METHOD_NPLUS1LOADER:
        load_option = nplus1loader('*')
    elif load_method is METHOD_SAFEGUARD:
        safeguard_session(ssn)
        extra_safeguard_query = 1  # always an extra query with this method

    # ### Test: deferred columns
    # Load objects; only one attribute undeferred
    numbers = load_numbers(ssn, sa.orm.load_only('id'), load_option)
    one = numbers[0]

    # potential stumbling block for the code that takes objects from the session
    # Also: it should not be autoflushed. And it will fail if it does (`id` is non-numeric)
    ssn.add(Number(id='Z'))

    # Make sure 'en' is unloaded
    with query_logger(ssn) as ql:
        assert not is_loaded_all('en', *numbers)
        assert ql.queries == 0  # no queries made here

    # Trigger a lazyload, see that one touch loads them all
    with query_logger(ssn) as ql:
        # Touch one object ; check the value
        assert touch_attribute('en', one) == ['one']

        # Query: one query to load them all
        assert ql.queries == 1 + extra_safeguard_query

        # Attribute is loaded on all other objects
        assert is_loaded_all('en', *numbers)

        # The other attribute is still unloaded
        assert not is_loaded_all('es', *numbers)

    # Make sure: no additional queries when accessing
    with query_logger(ssn) as ql:
        # touch ; check the value
        assert touch_attribute('en', *numbers) == ['one', 'two', 'three', 'four']

        # Made queries?
        if load_method == METHOD_NONE:
            assert ql.queries == (len(numbers)-1)
        else:
            assert ql.queries == 0  # no more queries



    # ### Test: modified values
    # nplus1loader shouldn't overwrite modified values
    ssn.expunge_all()

    numbers = load_numbers(ssn, sa.orm.load_only('id'), load_option)
    one, two = numbers[:2]

    # Modify a value
    with query_logger(ssn) as ql:
        one.en = 'wun'
        assert ql.queries == 0

    # Trigger a lazy-load
    with query_logger(ssn) as ql:
        # touch ; check the value
        assert touch_attribute('en', two) == ['two']

        # queries
        assert ql.queries == 1 + extra_safeguard_query

    # Make sure the modified value isn't overwritten
    assert one.en == 'wun'
    en: sa.orm.state.AttributeState = sa.orm.base.instance_state(one).attrs['en']
    assert en.value == 'wun'
    assert en.loaded_value == 'wun'
    assert en.history.added == ['wun']



    # ### Test: expired columns
    # NOTE: the loader currently does NOT handle expired classes, because *all* attributes are expired, not just one
    ssn.expunge_all()

    numbers = load_numbers(ssn, sa.orm.load_only('id'), load_option)
    one = numbers[0]
    ssn.add(Number())  # stumbling block

    # Expire some objects
    ssn.expire_all()

    # Touch expired attributes
    with query_logger(ssn) as ql:
        # touch ; check the value
        assert touch_attribute('en', one) == ['one']

        # No nplus1loading has taken place
        assert ql.queries == 1
        assert is_loaded_all('en', *numbers) == {True, False}

    # Manual refresh
    ssn.expire_all()
    with query_logger(ssn) as ql:
        ssn.refresh(one)  # manually

        # No nplus1loading has taken place
        assert ql.queries == 1
        assert is_loaded_all('en', *numbers) == {True, False}



    # ### Test: N+1 loading when there are no more instances
    ssn.expunge_all()

    one = ssn.query(Number).options(sa.orm.load_only('id'), load_option).first()

    # Touch
    with query_logger(ssn) as ql:
        assert touch_attribute('en', one) == ['one']

        # no errors

        assert ql.queries == 1  # no additional queries


def test_lazyload_relationship_without_fixes(ssn: sa.orm.Session, numbers_and_fruits: NumbersAndFruits):
    """ Baseline: column attribute lazy-load triggers many queries """
    numbers = load_numbers(ssn, sa.orm.load_only('id'))

    with query_logger(ssn) as ql:
        # Touch the `.en` on every Number
        touch_attribute('fruits', *numbers)

        # Result: one query per object
        assert len(ql) == len(numbers)
        assert len(ql) > 3  # sufficiently large to notice


@pytest.mark.parametrize('load_method', LOAD_METHODS)
def test_lazyload_scalar_relationship(ssn: sa.orm.Session, numbers_and_fruits: NumbersAndFruits, load_method):
    load_option = defaultload([])
    extra_safeguard_query = 0

    if load_method == METHOD_NPLUS1LOADER:
        # Node the `default_columns()` !
        load_option = default_columns(Fruit).nplus1loader('*')
    elif load_method == METHOD_SAFEGUARD:
        safeguard_session(ssn)
        extra_safeguard_query = 0  # for relationships, the current implementation does NOT generate additional queries

    # ### Test: load a relationship
    fruits = load_fruits(ssn, load_option)
    apple = fruits[0]
    ssn.add(Number())  # stumbling block

    assert not is_loaded_all('number', *fruits)

    # Trigger a lazyload
    with query_logger(ssn) as ql:
        # touch ; check the value
        assert apple.number.en == 'one'

        # One query
        assert ql.queries == 1 + extra_safeguard_query

        # All loaded
        assert is_loaded_all('number', *fruits)

    # Make sure: no additional queries when accessing
    with query_logger(ssn) as ql:
        numbers = touch_attribute('number', *fruits)
        assert [number.en if number else None for number in numbers] == ['one', 'one', 'two', 'two', 'three', 'three', None]

        # Made queries?
        if load_method == METHOD_NONE:
            assert ql.queries == 2  # only two unloaded Number-s were left
        else:
            assert ql.queries == 0  # no more queries


@pytest.mark.parametrize('load_method', LOAD_METHODS)
def test_lazyload_list_relationship(ssn: sa.orm.Session, numbers_and_fruits: NumbersAndFruits, load_method):
    """ Test: lazy-loading relationships: list """
    load_option = defaultload([])
    extra_safeguard_query = 0

    if load_method == METHOD_NPLUS1LOADER:
        # Node the `default_columns()` !
        load_option = default_columns(Number).nplus1loader('*')
    elif load_method == METHOD_SAFEGUARD:
        safeguard_session(ssn)
        extra_safeguard_query = 0  # for relationships, the current implementation does NOT generate additional queries


    # ### Test: load a relationship
    numbers = load_numbers(ssn, load_option)
    one = numbers[0]
    ssn.add(Number())  # stumbling block

    # Trigger a lazyload
    with query_logger(ssn) as ql:
        # touch ; check the value
        fruits = one.fruits
        assert [fruit.en for fruit in fruits] == ['apple', 'orange']

        assert ql.queries == 1 + extra_safeguard_query

        # all loaded
        assert is_loaded_all('fruits', *numbers)

    # Make sure: no additional queries when accessing
    with query_logger(ssn) as ql:
        fruits_values = touch_attribute('fruits', *numbers)
        assert [
            [fruit.en for fruit in fruits]
            for fruits in fruits_values
        ] == [
            ['apple', 'orange'],  # one
            ['grape', 'plum'],  # two
            ['cherry', 'strawberry'],  # three
            [],  # four
        ]

        # Made queries?
        if load_method == METHOD_NONE:
            assert ql.queries == (len(numbers)-1)
        else:
            assert ql.queries == 0  # no more queries


@pytest.mark.parametrize('load_method', LOAD_METHODS)
def test_lazyload_mapped_collection_relationship(ssn: sa.orm.Session, numbers_and_fruits: NumbersAndFruits, load_method):
    """ Test: lazy-loading relationships: mapped collection """
    load_option = defaultload([])
    extra_safeguard_query = 0

    if load_method == METHOD_NPLUS1LOADER:
        # Node the `default_columns()` !
        load_option = default_columns(Number).nplus1loader('*')
    elif load_method == METHOD_SAFEGUARD:
        safeguard_session(ssn)
        extra_safeguard_query = 0  # for relationships, the current implementation does NOT generate additional queries

    # ### Test: load a relationship
    numbers = load_numbers(ssn, load_option)
    one = numbers[0]

    # Trigger a lazyload
    with query_logger(ssn) as ql:
        # touch ; check the value
        fruits = one.fruits_map
        assert {key: fruit.en for key, fruit in fruits.items()} == {'apple': 'apple', 'orange': 'orange'}

        assert ql.queries == 1 + extra_safeguard_query

        # all loaded
        assert is_loaded_all('fruits_map', *numbers)

    # Make sure: no additional queries when accessing
    with query_logger(ssn) as ql:
        fruits_values = touch_attribute('fruits_map', *numbers)
        assert [
                   {key: fruit.en for key, fruit in fruits.items()}
                   for fruits in fruits_values
               ] == [
                   {'apple': 'apple', 'orange': 'orange'},  # one
                   {'grape': 'grape', 'plum': 'plum'},  # two
                   {'cherry': 'cherry', 'strawberry': 'strawberry'},  # three
                   {},  # four
               ]

        # Made queries?
        if load_method == METHOD_NONE:
            assert ql.queries == (len(numbers) - 1)
        else:
            assert ql.queries == 0  # no more queries


@pytest.mark.parametrize('load_method', LOAD_METHODS)
def test_lazyload_mapped_collection_relationship(ssn: sa.orm.Session, numbers_and_fruits: NumbersAndFruits, load_method):
    # TODO: convert the remaining test
    pass


def test_raiseload(ssn: sa.orm.Session, numbers_and_fruits: NumbersAndFruits):
    """ Test raiseload() """
    numbers = load_numbers(
        ssn,
        sa.orm.load_only('id'),
        raiseload_all('*')
    )
    one = numbers[0]

    # Check raises column
    with pytest.raises(LazyLoadingAttributeError):
        one.en

    # Check raises column again
    with pytest.raises(LazyLoadingAttributeError):
        one.en

    # Check raises relationship
    with pytest.raises(LazyLoadingAttributeError):
        one.fruits


# region Helpers


# A loader option that does nothing :)
DUMMY_LOAD_OPTION = defaultload([])


def is_loaded(attr_name: str, instance: object) -> bool:
    """ Check that an attribute is not loaded """
    state: sa.orm.state.InstanceState = sa.orm.base.instance_state(instance)
    return attr_name in state.dict


def is_loaded_all(attr_name: str, *instances: object) -> Union[bool, Set[bool]]:
    """ Check that an attribute is loaded for on objects. Returns the one value common to them all """
    values = {is_loaded(attr_name, instance) for instance in instances}
    if len(values) == 1:
        return values.pop()
    else:
        return values


def touch_attribute(attr_name: str, *instances: object) -> list:
    """ Get the value of the attribute on every instance """
    return [getattr(instance, attr_name) for instance in instances]


def load_numbers(ssn: sa.orm.Session, *options: sa.orm.Load) -> List[Number]:
    """ Load all Numbers from the db, with options() """
    return (
        ssn.query(Number)
            .options(*options)
            .order_by(Number.id.asc())  # predictable order
            .all()
    )


def load_fruits(ssn: sa.orm.Session, *options: sa.orm.Load) -> List[Number]:
    """ Load all Fruits from the db, with options() """
    return (
        ssn.query(Fruit)
            .options(*options)
            .order_by(Fruit.id.asc())
            .all()
    )


@contextmanager
def query_logger(ssn: sa.orm.Session) -> ContextManager[QueryLogger]:
    """ Log queries, check the final count """
    query_logger = QueryLogger(ssn.bind)

    # Clean-up (in case there was something)
    query_logger.clear()

    # Log
    with query_logger:
        yield query_logger


# endregion
