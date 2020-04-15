from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class Number(Base):
    """ Example model with columns and a 1-N relationship """
    __tablename__ = 'numbers'

    id = Column(Integer, primary_key=True, nullable=False)

    en = Column(String, nullable=True)
    es = Column(String, nullable=True)
    no = Column(String, nullable=True)

    fruits = relationship(lambda: Fruit, back_populates='number')


class Fruit(Base):
    """ Example model with columns and a N-1 relationship """
    __tablename__ = 'fruits'

    id = Column(Integer, primary_key=True, nullable=False)
    number_id = Column(Number.id.type, ForeignKey(Number.id), nullable=True)

    en = Column(String, nullable=True)
    es = Column(String, nullable=True)
    no = Column(String, nullable=True)

    number = relationship(lambda: Number, back_populates='fruits')
