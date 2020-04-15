""" An SqlAlchemy loader that solves the N+1 problem

TL;DR
=====

What happens if you touch an unloaded column or relationship while looping over results from the DB?

```python
users = ssn.query(User).all()

for user in users:
    user.articles  # load a relationship
```

Right. If you have 1000 users, you'll end up with 1000 queries.

Here's a simple solution for you that will lazy-load an attribute for all those users with just one query:

```python
from nplus1loader import default_columns, nplus1loader

users = ssn.query(User).options(
    default_columns(User),
    nplus1loader('*')
).all()

for user in users:
    user.articles
```

It will only make 1 query to load all the users, then when it sees that you want articles,
it will only make 1 additional query to load all articles¹ for those users.

¹: it will actually make 1 query per 500 users.

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

    from nplus1loader import nplus1loader

    users = ssn.query(User).options(
        load_only('id'),  # worst case scenario
        nplus1loader('*')  # include every column and every relationship
    )

Now, whenever you touch any unloaded column or relationship on any of those loaded users,
the attribute will be loaded for all User instance in that session.
So now, you can safely iterate through them.

Warning: do not rely on this mechanism; only use it as a safeguard for production.
It is recommended that you do something like this instead:

    from nplus1loader import default_columns, raiseload_all, nplus1loader

    users = ssn.query(User).options(
        default_columns(User),
        raiseload_all('*') if in_testing else nplus1loader('*')
    )

Make sure you import the module first in order for those options to get registered :)

    import nplus1loader

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

All logging is done to the 'lib.db.nplus1loader.NPlusOneLazyColumnLoader' logger:

    05:09:37 [W] nplus1loader.NPlusOneLazyColumnLoader: Number.es: N+1 loading of 4 instances
    emitted by: _nplusone_lazy_loading() nplus1loader/strategies.py:76

Other solutions
===============

* [operator/sqlalchemy_bulk_lazy_loader](https://github.com/operator/sqlalchemy_bulk_lazy_loader)
  only supports relationships and not columns, has code copy-pasted from an old SqlAlchemy version,
  requires re-configuring your relationships on the model (lazy='bulk')
* [operator/sqlalchemy_bulk_lazy_loader](operator/sqlalchemy_bulk_lazy_loader)
  is a monitoring tool; it does not solve the N+1 problem at all

Both projects served as a source of inspiration.



Database support
================

Tested with:

* PostgreSQL
* MySQL
* SQLite



Other Included Tools
====================

Handle specific attributes
--------------------------

The `nplus1loader` package has other tools that you might find useful.

First of all, you can be more specific about which attributes you want to have handled by the N+1 loader.
You can opt to handle only relationships, all, or specific ones:

    nplus1loader_rels('*')
    nplus1loader_rels(relationshop_name, ...)

or you can opt to handle only columns:

    nplus1loader_cols('*')
    nplus1loader_cols(column_name, ...)

`default_columns(Model)`
------------------------

This loader option takes the default defer()/undefer() settings from a model and sets them on a Query.
In itself, this option is useless, but it enables you to alter the defaults by using other loading options.

In particular, `nplus1loader('*')` won't work unless you provide `default_columns()` first.


`raiseload_all('*')`
--------------------

Raiseload for both columns and relationships.

Example:

    ssn.query(User).options(
        default_columns('id'),
        # Raise an error if any attribute at all is lazy-loaded
        raiseload_all('*')
    )

The error that it throws is `nplus1loader.LazyLoadingAttributeError()`, so it's easy to catch in your code ;)


`raiseload_col(*columns)`
-------------------------

Raiseload for columns. Use to fine-tune.

Example:

    ssn.query(User).options(
        load_only('id'),
        # Raise an error if any other column is lazy-loaded
        raiseload_col('*')
    )

You can also specify individual columns as a list.

The error that it throws is `nplus1loader.LazyLoadingAttributeError()`.


`raiseload_rel(*relations)`
---------------------------

Raiseload for relatioships. Use to fine-tune.

It's essentially the same as `sqlalchemy.orm.raiseload()`, but throws the `nplus1loader.LazyLoadingAttributeError()`
error instead.


`bulk_load_attribute_for_instance_states()`
-------------------------------------------

This is the heart of N+1 loader. You give it a list of instances, name one unlaoded attribute,
and it makes just one query to load this attribute's value from the database, and efficiently
augments all the existing instances with the loaded value of this attribute.
"""

# Loader options
from .loadopt import default_columns
from .loadopt import raiseload_col, raiseload_rel, raiseload_all
from .loadopt import nplus1loader, nplus1loader_cols, nplus1loader_rels

# Exceptions
from .exc import LazyLoadingAttributeError

# Low-level feature
from .bulk_load import bulk_load_attribute_for_instance_states
