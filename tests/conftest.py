from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from typing import Mapping

import pytest
import sqlalchemy as sa
import sqlalchemy.orm

from . import const, models


@pytest.fixture()
def ssn(engine: sa.engine.Engine) -> sa.orm.Session:
    # Clean the DB
    models.Base.metadata.drop_all(bind=engine)
    models.Base.metadata.create_all(bind=engine)

    # New session
    SessionMaker = sa.orm.sessionmaker(autocommit=False, autoflush=False, bind=engine)

    with closing(SessionMaker()) as ssn:
        yield ssn


@pytest.fixture()
def numbers_and_fruits(ssn: sa.orm.Session) -> NumbersAndFruits:
    from .models import Number, Fruit
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

    # Refresh all
    ssn.query(Number).all()
    ssn.query(Fruit).all()
    ssn.expunge_all()

    # Return
    return NumbersAndFruits(
        numbers={obj.id: obj for obj in ssn if isinstance(obj, Number)},
        fruits ={obj.id: obj for obj in ssn if isinstance(obj, Fruit)},
    )


@dataclass
class NumbersAndFruits:
    numbers: Mapping[str, models.Number]
    fruits: Mapping[str, models.Fruit]


@pytest.fixture(
    scope='module',
    params=[
        const.POSTGRES_URL,
        const.MYSQL_URL,
        'sqlite://',
    ]
)
def engine(request) -> sa.engine.Engine:
    DB_URL = request.param
    return sa.create_engine(DB_URL, echo=False)

