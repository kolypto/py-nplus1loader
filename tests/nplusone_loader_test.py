import unittest
from typing import List

from sqlalchemy import inspect
from sqlalchemy.orm import Session, load_only
from sqlalchemy.orm.state import InstanceState, AttributeState

from .db import init_database, drop_all, create_all
from .models import Base, Number, Fruit
from .query_logger import QueryLogger

from nplus1loader import nplus1loader, default_columns


class NPlusOneLoaderModelTest(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.engine, self.Session = init_database()
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
                default_columns(Number),  # !!!
                nplus1loader('*')
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

    def test_lazyload_scalar_relationship(self):
        """ Test the case where a scalar relationship is going to be lazy loaded """

        def main(ssn: Session, query_logger: QueryLogger, reset: callable):
            # ### Test: load a relationship
            reset()
            # one, two, three, four = load_numbers()
            apple, orange, grape, plum, cherry, strawberry, tomato = load_fruits(
                default_columns(Number),  # !!!
                nplus1loader('*')
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

