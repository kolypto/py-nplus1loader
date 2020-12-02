import logging
from functools import lru_cache
from typing import Set, Optional, Mapping

import sqlalchemy as sa
import sqlalchemy.event
import sqlalchemy.orm.base
import sqlalchemy.orm.state
import sqlalchemy.orm.strategies

from .bulk_load import bulk_load_attribute_for_instance_states
from .strategies import NPlusOneLazyColumnLoader
from .util import session_instances_with_unloaded_attribute


logger = logging.getLogger(__name__)



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

    # Skip relationships: in this implementation, they're handled by the nplus1loader
    # Explanation:
    #       One would expect that `InstanceEvents.refresh()` would be fired when any attribute is lazy-loaded,
    #       but in fact, it is not. It's fired for columns, but it's not fired for relationships.
    #       In short, `LazyLoader._emit_lazyload` does not fire `InstanceEvents.refresh`
    #
    #       For this reason, we've implemented an `InstanceEvents.load()` event handler
    #       and install a callable on every relatiosnhip property that handles nplus1loading properly.
    #       This loading, however, creates additional `InstanceEvents.refresh()` events that we do not want to handle.
    #
    #       For this reason, every `InstanceEvents.refresh()` event on a relationship attribute is ignored.
    attrs -= set(state.mapper.relationships.keys())

    # Okay, somebody is attempting to lazy-load an attribute on our watch.
    handle_lazy_load(session, state, attrs)


def handle_lazy_load(session: sa.orm.Session, state: sa.orm.state.InstanceState, attrs: Set[str]):
    # Load every attribute
    for attr in attrs:  # TODO: suboptimal when fields are many ; fix
        # Go through the Session and pick other instances where the very same attribute is unloaded
        # We're going to lazy-load all of them
        states = session_instances_with_unloaded_attribute(session, state.class_, attr)

        # Only load it if it hasn't been already loaded.
        # Who could've done this?
        # * another loader option
        # * nplus1loader() loader option; or
        # * us; when one field depends on another
        if not states:
            continue

        # Log
        if logger.isEnabledFor(logging.WARNING):
            states = list(states)
            logger.warning(
                "%s.%s: N+1 loading of %s instances",
                state.class_.__name__, attr,
                len(states)
            )

        # Now augment those instances with a bulk lazy-load
        # This function handles both attributes and relationships
        bulk_load_attribute_for_instance_states(session, state.mapper, states, attr, None)

# from sqlalchemy.orm.strategies import LazyLoader
#
# LazyLoader._original_emit_lazyload = LazyLoader._emit_lazyload
#
# def patched_emit_lazyload(*args):
#     self, session, state, = args[:3]
#
#     if is_safeguard_enabled(session):
#         handle_lazy_load(session, state, {self.key})
#
#     return LazyLoader._original_emit_lazyload(*args)
#
# LazyLoader._emit_lazyload = patched_emit_lazyload


@sa.event.listens_for(sa.orm.Mapper, 'load', named=True)
def on_instance_load(target: object, context: sa.orm.query.QueryContext):
    # Is safeguard enabled for this session?
    session: sa.orm.Session = context.session
    if not is_safeguard_enabled(session):
        return

    # Instance state
    state: sa.orm.state.InstanceState = sa.orm.base.instance_state(target)

    # Generate callables for relationships
    # This is not very optimal, but it's more reliable than hacking SqlAlchemy core.
    # TODO: find a reliable way to catch relationship lazy loads? For some reason, "refresh" event is not reported
    # when a relationship is lazyloaded by LazyLoader._emit_lazyload()
    callables = relationship_loader_callables_for(state.class_)

    if not state.callables:
        state.callables = callables.copy()
    else:
        for k in callables:
            if k not in state.callables:
                state.callables[k] = callables[k]


@lru_cache(typed=True)
def relationship_loader_callables_for(Model: type) -> Mapping:
    """ Create a dict of nplus1loader `callables` for unloaded relationships of `Model`

    This mapping is supposed to be merged into `InstanceState.callables` and lazy-load unloaded relationships
    """
    mapper: sa.orm.Mapper = sa.orm.base.class_mapper(Model)
    return {
        name: NPlusOneLazyColumnLoader(relationship, (('lazy', 'nplus1'),))._nplus1_lazy_loading
        for name, relationship in mapper.relationships.items()
    }

# endregion
