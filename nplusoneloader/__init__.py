""" An SqlAlchemy loader that solves the N+1 problem

The N+1 Problem
===============

First of all, what's the N+1 problem?
Suppose you have the following code which loads a number of objects from the DB,
but some fields are deferred:

    users = ssn.query(User).options(
        load_only('id')  # worst case scenario
    )

And then somewhere in the code some `jsonify()` function decides to check one of
the unloaded properties:

    for user in users:
        user.state  # load the attribute

Because the attribute hasn't been loaded on the whole collection...
it will trigger N queries to load that attribute on every iteration of the loop.
Crying shame.

In many cases we can tell SqlAlchemy to load exactly the attributes we are going
to use, but in other cases that's utterly impossible.

The Solution
============

This module offers a solution: when your code *touches* one unloaded attribute
on a collection, it assumes that you're going to iterate through them, and loads
all of them for you.

The solution is, however, not all-encompassing: you only enable it where you see fit.
It's implemented as a load option, like this:

    users = ssn.query(User).options(
        load_only('id'),  # worst case scenario
        nplus1loader('*')  # include every column and every relationship
    )

Now, whenever you touch any unloaded column or relationship on any of those loaded users,
the attribute will be loaded for all User instance in that session.
So now, you can safely iterate through them.

Warning: do not rely on this mechanism; only use it as a safeguard for production.
It is recommended that you do something like this instead:

    users = ssn.query(User).options(
        default_columns(User),
        raiseload('*') if in_testing else nplus1loader('*')
    )

Make sure you import the module first in order for those options to get registered :)

    import nplusoneloader

One minor inconvenience
=======================

Did you notice the `default_columns()` load option?
You'll have to insert it early into your query, I'm sorry.

This is because nplus1loader('*') is a catch-all column loader option.
In other words, if no column strategies are given by you, there are no loaded columns,
and this nplus1loader('*') will handle all columns!

This is not what we want. We would instead want to apply the defaults... then override them...
and put this nplus1loader('*') in the end. But that's not possible with SqlAlchemy.

So we have to live with this default_columns(Model): it reads the default loading strategies
from your Model and applies it early. In other words, it knows which columns are deferred by default.

If you don't put it there, nplus1loader() will remove all columns from the query, and you'll get
this weird error:

    sqlalchemy.exc.InvalidRequestError: Query contains no columns with which to SELECT from.

This is effectively the same as `defer('*')`.

Now... why can't we just undefer('*') everything?
Because that's not right with some Column(deferred=True), and Column(deferred_group='password'),
which have other preferences about being loaded.

Logging
=======

All logging is done to the 'lib.db.nplusone_loader.NPlusOneLazyColumnLoader' logger:

    05:09:37 [W] lib.db.nplusone_loader.NPlusOneLazyColumnLoader: Number.es: N+1 loading of 4 instances
    emitted by: _nplusone_lazy_loading() nplusoneloader/strategies.py:76

Other solutions
===============

* [operator/sqlalchemy_bulk_lazy_loader](https://github.com/operator/sqlalchemy_bulk_lazy_loader)
  only supports relationships and not columns, has code copy-pasted from an old SqlAlchemy version,
  requires re-configuring your relationships on the model (lazy='bulk')
* [operator/sqlalchemy_bulk_lazy_loader](operator/sqlalchemy_bulk_lazy_loader)
  is a monitoring tool; it does not solve the N+1 problem at all

Both projects served as a source of inspiration.


Limitations
===========

Currently, only works with PostgreSQL.

There should be no problem to ensure its operation for other databases, because the only place where
it depends on Postgres is the `bulk_load.py`, where it uses tuples to make very optimal IN queries.

"""

# Loader options
from .loadopt import default_columns
from .loadopt import nplus1loader, nplus1loader_cols, nplus1loader_rels

# Low-level feature
from .bulk_load import bulk_load_attribute_for_instance_states
