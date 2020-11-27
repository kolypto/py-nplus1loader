import unittest
from typing import List

from sqlalchemy import inspect
from sqlalchemy.orm import Session, load_only, joinedload
from sqlalchemy.orm.state import InstanceState, AttributeState

from nplus1loader import nplus1loader, default_columns, raiseload_all, LazyLoadingAttributeError
from nplus1loader.util import query_nplus1loader_others
from . import const
from .db import init_database, drop_all, create_all
from .models import Base, Number, Fruit
from .query_logger import QueryLogger


class NPlusOneLoaderPostgresTest(unittest.TestCase):
    DB_URL = const.POSTGRES_URL

    def setUp(self):
        super().setUp()

        self.engine, self.Session = init_database(url=self.DB_URL)
        drop_all(self.engine, Base)
        create_all(self.engine, Base)

        # Insert some things into the DB
        # noinspection PyArgumentList
        ssn = self.Session()
        ssn.begin()
        ssn.add_all([
            # Three Numbers, each with 2 Fruits
            Number(id=1, en='one', es='uno', no='en', fruits=[
                Fruit(id=11, en='apple', es='manzana', no='manzana'),
                Fruit(id=12, en='orange', es='naranja', no='oransje'),
            ]),
            Number(id=2, en='two', es='dos', no='to', fruits=[
                Fruit(id=21, en='grape', es='uva', no='drue'),
                Fruit(id=22, en='plum', es='ciruela', no='plomme'),
            ]),
            Number(id=3, en='three', es='tres', no='tre', fruits=[
                Fruit(id=31, en='cherry', es='cereza', no='kirsebær'),
                Fruit(id=32, en='strawberry', es='fresa', no='jordbær'),
            ]),
            # One Number with no Fruits
            Number(id=4, en='four', es='cuatro', no='fire'),
            # One Fruit with no Number
            Fruit(id=40, en='tomato', es='tomate', no='tomat'),
        ])
        ssn.commit()
        ssn.close()

        # QueryLogger
        self.query_logger = QueryLogger(self.engine)

    def test_lazyload_column(self):
        """ Test the case where a column is going to be lazy loaded """
        def main(ssn: Session, query_logger: QueryLogger, reset: callable):
            # ### Test: load a deferred column without the solution
            reset()
            numbers = load_numbers_only_id()
            with query_logger:
                # Touch the `.en` on every Number
                [number.en for number in numbers]

                # a query has been made for every number.
                # That's the N+1 problem.
                self.assertMadeQueries(len(numbers))

            # ### Test: load a deferred column
            reset()
            # Load
            one, two, three, four = load_numbers_only_id(
                nplus1loader('*')
            )
            ssn.add(Number(id='Z'))  # potential stumbling block. It should not be autoflushed. And it will fail if it does.

            with query_logger:
                # Make sure 'en' is unloaded
                self.assertUnloaded('en', one)
                self.assertUnloaded('en', two)
                self.assertUnloaded('en', three)
                self.assertUnloaded('en', four)

                # Trigger a lazyload of `en`
                one.en
                self.assertMadeQueries(1)  # one query to load them all
                self.assertLoaded('en', one)
                self.assertLoaded('en', two)
                self.assertLoaded('en', three)
                self.assertLoaded('en', four)

                # Freely access the `en` attribute on other objects: no additional queries
                self.assertEqual(one.en, 'one')
                self.assertEqual(two.en, 'two')
                self.assertEqual(three.en, 'three')
                self.assertMadeQueries(0)  # no additional queries

                # Make sure 'es' is unloaded
                self.assertUnloaded('es', one)
                self.assertUnloaded('es', two)
                self.assertUnloaded('es', three)
                self.assertUnloaded('es', four)

                # Trigger a lazyload of `es`
                two.es
                self.assertMadeQueries(1)  # one query to load them all
                self.assertLoaded('es', one)
                self.assertLoaded('es', two)
                self.assertLoaded('es', three)
                self.assertLoaded('es', four)

                # Freely access the `es` attribute
                self.assertEqual(one.es, 'uno')
                self.assertEqual(two.es, 'dos')
                self.assertEqual(three.es, 'tres')
                self.assertMadeQueries(0)  # no additional queries

            # ### Test: lazy load does not overwrite a changed value
            reset()
            one, two, three, four = load_numbers_only_id()

            with query_logger:
                # Change one.en
                # Note that the previous value remains unloaded!
                one.en = 'wun'  # Vietnamese accent
                self.assertMadeQueries(0)  # no queries

                # Make sure it's still unloaded
                one_state: InstanceState = inspect(one)
                ien: AttributeState = one_state.attrs['en']
                self.assertNotIn('en', one_state.unloaded)
                self.assertEqual(ien.value, 'wun')
                self.assertEqual(ien.loaded_value, 'wun')
                self.assertEqual(ien.history.added, ['wun'])
                self.assertEqual(ien.history.unchanged, ())
                self.assertEqual(ien.history.deleted, ())

                # Trigger a lazyload
                two.en
                self.assertMadeQueries(1)  # one query to load them all

                # Make sure the value's not overwritten
                self.assertEqual(one.en, 'wun')

                # Make sure it's loaded, but not overwritten
                one_state: InstanceState = inspect(one)
                ien: AttributeState = one_state.attrs['en']
                self.assertNotIn('en', one_state.unloaded)
                self.assertEqual(ien.value, 'wun')
                self.assertEqual(ien.loaded_value, 'wun')
                self.assertEqual(ien.history.added, ['wun'])
                self.assertEqual(ien.history.unchanged, ())
                self.assertEqual(ien.history.deleted, ())

            # ### Test: load an expired column
            reset()
            one, two, three, four = load_numbers()
            ssn.add(Number())  # stumbling block

            with query_logger:
                # TODO: it doesn't currently handle expired items (because *all* attributes are expired, not just one)
                ssn.expire(one)
                ssn.expire(two)
                ssn.expire(three)

                # Access a loaded attr: no additional queries
                self.assertEqual(four.en, 'four')
                self.assertMadeQueries(0)

                # Touch an expired attribute
                self.assertEqual(one.en, 'one')
                self.assertMadeQueries(1)  # loaded

                # Touch another expired attribute
                self.assertEqual(one.es, 'uno')
                self.assertMadeQueries(0)  # all refreshed

                # Freely touch attributes on other expired
                self.assertEqual(one.en, 'one')
                self.assertEqual(two.en, 'two')
                self.assertEqual(three.en, 'three')
                self.assertMadeQueries(2)  # loading for every expired item

            # ### Test: manual refresh
            reset()
            one, two, three, four = load_numbers()
            with query_logger:
                ssn.expire(one)
                ssn.expire(two)
                ssn.expire(three)

                # Now do a manual refresh
                ssn.refresh(three)
                self.assertMadeQueries(1)  # refreshed

                # Make sure others are still unloaded
                self.assertUnloaded('en', one)


            # ### Test: N+1 loading when there are no more instances
            reset()
            one = ssn.query(Number).options(
                load_only('id'),
                nplus1loader('*')
            ).first()

            with query_logger:
                # Make sure 'en' is unloaded
                self.assertUnloaded('en', one)

                # Trigger a lazyload of `en`
                one.en
                self.assertMadeQueries(1)

                # Actually loaded
                self.assertLoaded('en', one)
                self.assertEqual(one.en, 'one')


        def load_numbers(*options) -> List[Number]:
            return ssn.query(Number).options(*options).order_by(Number.id.asc()).all()

        def load_numbers_only_id(*options) -> List[Number]:
            return ssn.query(Number).options(
                load_only('id'),
                *options
            ).order_by(Number.id.asc()).all()

        ssn = self.Session()
        self._run_main(main, ssn)

    def test_lazyload_list_relationship(self):
        """ Test the case where a list relationship is going to be lazy loaded """
        def main(ssn: Session, query_logger: QueryLogger, reset: callable):
            # ### Test: load a relationship without the solution
            reset()
            numbers = load_numbers()
            with query_logger:
                # Touch the `.fruits` on every Number
                [number.fruits for number in numbers]

                # a query has been made for every number.
                # That's the N+1 problem.
                self.assertMadeQueries(len(numbers))

            # ### Test: load a relationship
            reset()
            # one, two, three, four = load_numbers()
            one, two, three, four = load_numbers(
                default_columns(Number)  # !!!
                    .nplus1loader('*')
            )
            ssn.add(Number())  # stumbling block

            with query_logger, ssn.no_autoflush:
                # Make sure it's unloaded
                self.assertUnloaded('fruits', one)
                self.assertUnloaded('fruits', two)
                self.assertUnloaded('fruits', three)
                self.assertUnloaded('fruits', four)

                # Trigger a lazyload of `fruits`
                one.fruits
                self.assertMadeQueries(1)  # one query to load them all
                self.assertLoaded('fruits', one)
                self.assertLoaded('fruits', two)
                self.assertLoaded('fruits', three)
                self.assertLoaded('fruits', four)

                # Freely access the `fruits` attribute on other objects: no additional queries
                self.assertIsInstance(one.fruits, list)
                self.assertIsInstance(two.fruits, list)
                self.assertIsInstance(three.fruits, list)
                self.assertIsInstance(four.fruits, list)
                self.assertEqual(len(one.fruits), 2)
                self.assertEqual(len(two.fruits), 2)
                self.assertEqual(len(three.fruits), 2)
                self.assertEqual(len(four.fruits), 0)
                self.assertMadeQueries(0)  # no additional queries

        def load_numbers(*options) -> List[Number]:
            return ssn.query(Number).options(*options).order_by(Number.id.asc()).all()

        ssn = self.Session()
        self._run_main(main, ssn)

    def test_lazyload_mapped_collection_relationship(self):
        """ Test the case where a MappedCollection relationship is going to be lazy loaded """
        def main(ssn: Session, query_logger: QueryLogger, reset: callable):
            # ### Test: load a relationship without the solution
            reset()
            numbers = load_numbers()
            with query_logger:
                # Touch the `.fruits_map` on every Number
                [number.fruits_map for number in numbers]

                # a query has been made for every number.
                # That's the N+1 problem.
                self.assertMadeQueries(len(numbers))

            # ### Test: load a relationship
            reset()
            # one, two, three, four = load_numbers()
            one, two, three, four = load_numbers(
                default_columns(Number)  # !!!
                    .nplus1loader('*')
            )
            ssn.add(Number())  # stumbling block

            with query_logger, ssn.no_autoflush:
                # Make sure it's unloaded
                self.assertUnloaded('fruits_map', one)
                self.assertUnloaded('fruits_map', two)
                self.assertUnloaded('fruits_map', three)
                self.assertUnloaded('fruits_map', four)

                # Trigger a lazyload of `fruits_map`
                one.fruits_map
                self.assertMadeQueries(1)  # one query to load them all
                self.assertLoaded('fruits_map', one)
                self.assertLoaded('fruits_map', two)
                self.assertLoaded('fruits_map', three)
                self.assertLoaded('fruits_map', four)

                # Freely access the `fruits_map` attribute on other objects: no additional queries
                self.assertIsInstance(one.fruits_map, dict)
                self.assertIsInstance(two.fruits_map, dict)
                self.assertIsInstance(three.fruits_map, dict)
                self.assertIsInstance(four.fruits_map, dict)
                self.assertEqual(len(one.fruits_map), 2)
                self.assertEqual(len(two.fruits_map), 2)
                self.assertEqual(len(three.fruits_map), 2)
                self.assertEqual(len(four.fruits_map), 0)
                self.assertMadeQueries(0)  # no additional queries

        def load_numbers(*options) -> List[Number]:
            return ssn.query(Number).options(*options).order_by(Number.id.asc()).all()

        ssn = self.Session()
        self._run_main(main, ssn)

    def test_lazyload_scalar_relationship(self):
        """ Test the case where a scalar relationship is going to be lazy loaded """

        def main(ssn: Session, query_logger: QueryLogger, reset: callable):
            # ### Test: load a relationship
            reset()
            # one, two, three, four = load_numbers()
            apple, orange, grape, plum, cherry, strawberry, tomato = load_fruits(
                default_columns(Number)  # !!!
                    .nplus1loader('*')
            )
            ssn.add(Number())  # stumbling block

            with query_logger, ssn.no_autoflush:
                # Make sure it's unloaded
                self.assertUnloaded('number', apple)
                self.assertUnloaded('number', orange)
                self.assertUnloaded('number', grape)
                self.assertUnloaded('number', plum)
                self.assertUnloaded('number', cherry)
                self.assertUnloaded('number', strawberry)
                self.assertUnloaded('number', tomato)

                # Trigger a lazyload of `fruits`
                apple.number
                self.assertMadeQueries(1)  # one query to load them all
                self.assertLoaded('number', apple)
                self.assertLoaded('number', orange)
                self.assertLoaded('number', grape)
                self.assertLoaded('number', plum)
                self.assertLoaded('number', cherry)
                self.assertLoaded('number', strawberry)
                self.assertLoaded('number', tomato)

                # Freely access the `fruits` attribute on other objects: no additional queries
                self.assertEqual(apple.number.en, 'one')
                self.assertEqual(orange.number.en, 'one')
                self.assertEqual(grape.number.en, 'two')
                self.assertEqual(plum.number.en, 'two')
                self.assertEqual(cherry.number.en, 'three')
                self.assertEqual(strawberry.number.en, 'three')
                self.assertEqual(tomato.number, None)
                self.assertMadeQueries(0)  # no additional queries

        def load_fruits(*options) -> List[Fruit]:
            return ssn.query(Fruit).options(*options).order_by(Fruit.id.asc()).all()

        ssn = self.Session()
        self._run_main(main, ssn)

    def test_lazyload_nested(self):
        """ Test what happens when a relationship loaded by nplus1loader also triggers a lazyload """

        def main(ssn: Session, query_logger: QueryLogger, reset: callable):
            with query_logger, ssn.no_autoflush:
                # ### Test: load a relationship's relationship
                # In this test we'll observe that nplus1loader('*') only handles top-level relationships
                # and does not handle second-level relationships
                fruits = load_fruits(
                    nplus1loader('*', nested=False)
                )
                self.assertMadeQueries(1)  # made 1 query to load them all

                # Now iterate it's first-level attribute
                iterate_nested_relationship(fruits)
                self.assertMadeQueries(1 + 3)  # one query to load them all + a query per fruit (where a number is set)

                # ### Test: see how nplus1loader('*') loads second-level relationships
                reset()
                fruits = load_fruits(
                    nplus1loader('*', nested=True)
                )
                self.assertMadeQueries(1)  # made 1 query to load them all

                # Now iterate it's first-level attribute
                iterate_nested_relationship(fruits)
                self.assertMadeQueries(2)  # one query to load `fruit.number`, one more query to load `fruit.number.fruits`

                # ### Test: chained options
                reset()
                fruits = load_fruits(
                    joinedload(Fruit.number)
                        .default_columns(Number)
                        .nplus1loader('*')
                )
                self.assertMadeQueries(1)  # made 1 query to load them all

                iterate_nested_relationship(fruits)
                self.assertMadeQueries(1)  # just 1 query to load fruit.number.fruits

                # ### Test: nested un-N+1-ed relations
                reset()
                fruits = load_fruits(
                    # Let's assume some library has quietly added this relationship to be loaded
                    # There's no N+1 loader set on it
                    joinedload(Fruit.number),
                    # But there's an N+1 loader at the top
                    default_columns(Fruit).nplus1loader('*', others=False)
                )
                self.assertMadeQueries(1)  # query

                iterate_nested_relationship(fruits)
                self.assertMadeQueries(3)  # every fruit.number.fruits loaded individually.
                # In this case, every relationship starting with `joinedload()` has no N+1 loader installed.
                # Therefore, Fruit.number is loaded, but everything beyond that is lazy-loaded.
                # In order to defeat this, we'll need to manually alter the loader options that are already on the query

                # ### Test: nested un-N+1-ed relations fixed (no)
                reset()
                fruits = load_fruits(
                    # Let's assume some library has quietly added this relationship to be loaded
                    # There's no N+1 loader set on it
                    joinedload(Fruit.number),
                    # But there's an N+1 loader at the top
                    nplus1loader('*', others=True),
                )
                self.assertMadeQueries(1)  # query

                iterate_nested_relationship(fruits)
                self.assertMadeQueries(3)  # N+1.... :(
                # Bad. This test has made 3 queries, but why?
                # This is because there's nothing deferred on our Query, and SqlAlchemy is being lazy about it:
                # it does not invoke `NPlusOneLazyColumnLoader.setup_query()`, and so the Query's loading isn't fixed.
                # Let's try again with a deferred column

                # ### Test: nested un-N+1-ed relations fixed (yes)
                reset()
                query = ssn.query(Fruit).options(
                    # Let's assume some library has quietly added this relationship to be loaded
                    # There's no N+1 loader set on it
                    joinedload(Fruit.number),
                    # But there's an N+1 loader at the top
                    default_columns(Fruit).nplus1loader('*', others=True)  # others=True
                )

                # TODO: this is ugly and isn't supposed to be like that.
                #  Drop this line and find out why `others=True` doesn't get applied.
                #  Perhaps it somehow has to be postponed until query execution time?
                # The problem here: setup_query() isn't called when we have no deferred columns
                # (because SqlAlchemy uses lazy logic with loading options, and it our option doesn't apply to anything,
                # it's not processed).
                # But even if it is called, it somehow fails to apply.
                # Why? No idea. Either because it happens too early, or because those changes get lost somehow.
                # Need to investigate. Until then, here's an ugly utility function for you ;)
                query_nplus1loader_others(query)

                # TEMPORARY: test code to pretty-print all the loader options
                for key, loader in query._attributes.items():
                    if isinstance(key, tuple):
                        path = key[1]
                        print(path, loader.strategy)

                fruits = query.all()
                self.assertMadeQueries(1)  # query

                iterate_nested_relationship(fruits)
                self.assertMadeQueries(1)  # GOOD! Just one N+1-loader query


        def load_fruits(*options) -> List[Fruit]:
            return ssn.query(Fruit).options(
                default_columns(Fruit),
                *options
            ).all()

        def iterate_nested_relationship(fruits: List[Fruit]):
            for fruit in fruits:
                # Touch the 1st level relationship
                if fruit.number:
                    # Touch the 2nd level relationship
                    fruit.number.fruits

        ssn = self.Session()
        self._run_main(main, ssn)

    def test_raiseload(self):
        """ Test raiseload """
        ssn = self.Session()

        # Load
        one, two, three, four = ssn.query(Number).options(
            load_only('id'),
            raiseload_all('*')
        ).all()

        # Check raises column
        with self.assertRaises(LazyLoadingAttributeError):
            one.en

        # Check raises column again
        with self.assertRaises(LazyLoadingAttributeError):
            one.en

        # Check raises relationship
        with self.assertRaises(LazyLoadingAttributeError):
            one.fruits


    # Helpers

    def assertMadeQueries(self, expected_query_count) -> List[str]:
        """ Assert that the QueryLogger contains exactly `expected_query_count` SQL queries

        Returns:
            The list of logged queries
        """
        try:
            self.assertEqual(len(self.query_logger), expected_query_count)
            return list(self.query_logger)
        except AssertionError:
            print('=== Queries')
            print('\n\n\n'.join(self.query_logger))
            raise
        finally:
            self.query_logger.clear()

    def assertLoaded(self, attr_name: str, instance: object):
        """ Assert that an attribute is loaded on an instance """
        return self.assertNotIn(attr_name, inspect(instance).unloaded)

    def assertUnloaded(self, attr_name: str, instance: object):
        """ Assert that an attribute is unloaded on an instance """
        return self.assertIn(attr_name, inspect(instance).unloaded)

    def _run_main(self, main: callable, ssn: Session):
        """ Run a main() function with a dependency injection """
        query_logger = self.query_logger

        def reset():
            ssn.expunge_all()
            query_logger.clear()

        main(ssn, query_logger, reset)


class NplusOneLoaderMySQLTest(NPlusOneLoaderPostgresTest):
    """ Test nplus1loader() with MySQL """
    DB_URL = const.MYSQL_URL


class NplusOneLoaderSQLiteTest(NPlusOneLoaderPostgresTest):
    """ Test nplus1loader() with SQLite """
    DB_URL = 'sqlite://'
