from contextlib import contextmanager

import pytest
import sqlalchemy as sa
import sqlalchemy.event
import sqlalchemy.orm
import sqlalchemy.orm.base
import sqlalchemy.orm.attributes
import sqlalchemy.orm.events

from .conftest import NumbersAndFruits
from .models import Base, Number, Fruit


@pytest.mark.skip('not a test')
def test_playground(ssn: sa.orm.Session, numbers_and_fruits: NumbersAndFruits):
    with print_all_sqlalchemy_events():
        numbers = ssn.query(Number).options(
            sa.orm.load_only('id')
        ).all()

        print('\n'*5)

        s = numbers[0]._sa_instance_state
        print(s.callables)

        # numbers[0].en
        numbers[0].fruits

    pytest.fail(':)')


def test_play(ssn: sa.orm.Session, numbers_and_fruits: NumbersAndFruits):
    one = ssn.query(Number).options(sa.orm.load_only('id')).filter(Number.en == 'one').one()

    ssn.query(Number).options(
        sa.orm.joinedload(Number.fruits)
    ).filter(
        Number.en == 'one'
    ).all()


@contextmanager
def print_all_sqlalchemy_events():
    """ Subscribe to all SqlAlchemy events and print them """
    with TapEvents() as events:
        # for Model in Base.__subclasses__():
        #     Mapper: sa.orm.Mapper = sa.orm.base.class_mapper(Model)
        #     for attribute in Mapper.all_orm_descriptors:
        #         events.tap_all(attribute, sa.orm.events.AttributeEvents)
        events.tap(sa.orm.attributes.InstrumentedAttribute, 'set')

        events.tap_all(sa.orm.Mapper, sa.orm.events.MapperEvents)
        events.tap_all(sa.orm.Mapper, sa.orm.events.InstanceEvents)
        events.tap_all(sa.orm.Session, sa.orm.events.SessionEvents, propagate=True)
        events.tap_all(sa.orm.Query, sa.orm.events.QueryEvents, propagate=True)
        events.tap_all(type, sa.orm.events.InstrumentationEvents, propagate=True)

        yield


class TapEvents:
    """ A collection of events that are listened to and printed """
    def __init__(self):
        self._events = []
        self._subscribe = False

    def tap(self, target, event_name: str, propagate: bool = False):
        listener = TapEvent(target, event_name, propagate)
        self._events.append(listener)
        if self._subscribe:
            listener.subscribe()
        return self

    def tap_all(self, target, EventClass: type, propagate: bool = False):
        # Find event names
        event_names = {
            name
            for name in dir(EventClass)
            if not name.startswith('_') and name != 'dispatch'
        }

        # Add
        for event_name in event_names:
            self.tap(target, event_name, propagate)

        return self

    def __enter__(self):
        self.subscribe()
        return self

    def __exit__(self, *exc):
        self.unsubscribe()

    def subscribe(self):
        self._subscribe = True
        for event in self._events:
            event.subscribe()

    def unsubscribe(self):
        self._subscribe = False
        for event in self._events:
            event.unsubscribe()


class SAEventListener:
    """ SqlAlchemy event listener """
    def __init__(self, target, event_name: str, propagate: bool = False):
        self.target = target
        self.event_name = event_name
        self.propagate = propagate
        self._subscribed = False

    def subscribe(self):
        sa.event.listen(self.target, self.event_name, self.handler, named=True, propagate=self.propagate)
        self._subscribed = True

    def unsubscribe(self):
        if self._subscribed:
            sa.event.remove(self.target, self.event_name, self.handler)
            self._subscribed = False

    def handler(self, **kwargs):
        raise NotImplementedError


class TapEvent(SAEventListener):
    """ SA Event listener: print """

    def handler(self, **kwargs):
        print(f'🎀🎀🎀 EVENT on {self.target} : {self.event_name!r} ; args={kwargs!r}')
