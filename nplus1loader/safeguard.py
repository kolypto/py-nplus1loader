from typing import Set, Optional

import sqlalchemy as sa
import sqlalchemy.event
import sqlalchemy.orm.base
import sqlalchemy.orm.state

from .bulk_load import bulk_load_attribute_for_instance_states
from .util import session_instances_with_unloaded_attribute


def safeguard_session(ssn: sa.orm.Session):
    """ Enable the safeguard for this session """
    ssn.info[SESSION_MARKER] = True


def safeguard_session_disable(ssn: sa.orm.Session):
    """ Disable the safeguard for this session """
    ssn.info.pop(SESSION_MARKER, False)


def is_safeguard_enabled(ssn: sa.orm.Session) -> bool:
    """ Has safeguard_session() been called on this session? """
    return ssn.info.get(SESSION_MARKER, False)


SESSION_MARKER = f':nplus1loader:safeguard'


# region Event listeners

@sa.event.listens_for(sa.orm.Mapper, 'refresh', named=True)
def on_instance_refresh(target: object, context: sa.orm.query.QueryContext, attrs: Optional[Set[str]]):
    # When `attrs` is None, then the whole instance is refreshing.
    # That's not a lazy-load; don't handle
    if attrs is None:
        return

    # Is safeguard enabled for this session?
    session: sa.orm.Session = context.session
    if not is_safeguard_enabled(session):
        return

    # When the instance is expired, it's the same as having `attrs=None`: all attributes are being loaded.
    # Skip.
    state: sa.orm.state.InstanceState = sa.orm.base.instance_state(target)
    if state.expired:
        return

    # Okay, somebody is attempting to lazy-load an attribute on our watch.

    # Load every attribute
    for attr in attrs:
        # Go through the Session and pick other instances where the very same attribute is unloaded
        # We're going to lazy-load all of them
        states = session_instances_with_unloaded_attribute(session, state.class_, attr)

        # Now augment those instances with a bulk lazy-load
        # This function handles both attributes and relationships
        bulk_load_attribute_for_instance_states(session, state.mapper, states, attr, None)

# endregion
