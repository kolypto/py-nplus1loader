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
