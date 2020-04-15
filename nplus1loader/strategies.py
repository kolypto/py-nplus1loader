""" The implementation of the N+1 Loading strategy """

from typing import Iterable, Mapping

from sqlalchemy import log
from sqlalchemy.engine import ResultProxy
from sqlalchemy.orm.base import instance_state
from sqlalchemy.orm.query import QueryContext
from sqlalchemy.orm.state import InstanceState
from sqlalchemy.orm import ColumnProperty, RelationshipProperty, Mapper, Session
from sqlalchemy.orm.strategy_options import Load
from sqlalchemy.orm.strategies import LoaderStrategy

try:
    # In sqlalchemy>=1.3.13, it's something else
    from sqlalchemy.orm.path_registry import AbstractEntityRegistry
except ImportError:
    # In sqlalchemy<=1.3.12 it's called an `Entity Registry`
    from sqlalchemy.orm.path_registry import EntityRegistry as AbstractEntityRegistry

from .exc import LazyLoadingAttributeError
from .bulk_load import bulk_load_attribute_for_instance_states



# This class catch the loader options you've just declared using some clever dict matching technique:
# they react to strategized attributes configured with {'nplus1': True} or {'lazy': 'nplus1'}
# The former is used for columns, the latter -- for relationships,
# and they are both loaded using this particular class.

@log.class_logger
@ColumnProperty.strategy_for(nplus1=True)
@RelationshipProperty.strategy_for(lazy="nplus1")
class NPlusOneLazyColumnLoader(LoaderStrategy):
    """ Lazy loader for the N+1 situation """

    def create_row_processor(self, context: QueryContext, path: AbstractEntityRegistry, loadopt: Load, mapper: Mapper,
                             result: ResultProxy, adapter, populators: Mapping[str, list]):
        # This method prepares the `populators` dict
        # It's undocumented in SqlAlchemy, but it seems like that's where you store callables
        # that handle the loading of deferred attributes.
        #
        # Our attribute name is `self.key`, which is the name of the model attribute,
        # and our callable is `self._nplusone_lazy_loading` method.
        # It's going to be called when the attribute is deferred, and touched by some-unsuspecting-body

        # Adapted from sqlalchemy.orm.strategies.LazyLoader.create_row_processor
        # The end result of all this magic is to have our `self._nplus1_lazy_loading` callable
        # inserted into InstanceState.callables[self.key]
        # This will ensure it'll get called when the attribute is lazy loaded
        set_lazy_callable = (
            InstanceState._instance_level_callable_processor
        )(mapper.class_manager, self._nplus1_lazy_loading, self.key)

        # I'm not certain that "new" is the right key. Other options:
        # "new", "expired", "quick", "delayed", "existing", "eager"
        # Seems like all these are scenarios under which an attribute may be accessed
        populators["new"].append((self.key, set_lazy_callable))

    def _nplus1_lazy_loading(self, state: InstanceState, passive='NOT USED'):
        """ Handle the lazy-loading for an attribute on a particular instance

        Args:
            state: The instance whose attribute is being lazy loaded
        """
        mapper: Mapper = self.parent
        session = state.session

        # Okay, somebody is attempting to lazy-load an attribute on our watch.
        # First, go through the Session and pick other instances where the very same attribute is unloaded
        # We're going to lazy-load all of them
        states = self._get_instance_states_with_unloaded(session, mapper, self.key)

        # Log
        if self._should_log_info:
            states = list(states)
            self.logger.warn(
                "%s.%s: N+1 loading of %s instances",
                mapper.class_.__name__,
                self.key,
                len(states)
            )

        # Now augment those instances with a bulk lazy-load
        # This function handles both attributes and relationships
        bulk_load_attribute_for_instance_states(session, mapper, states, self.key)

        # Finally, return the new value of the attribute.
        # bulk loader has already set it, actually... but the row processor contract requires that we return it.

        # This monstrous thing is the right way to get the "committed value" from an sqlalchemy instance :)
        return state.get_impl(self.key).get_committed_value(state, state.dict)

    @staticmethod
    def _get_instance_states_with_unloaded(session: Session, mapper: Mapper, attr_name: str) -> Iterable[InstanceState]:
        """ Iterate over instances in the `session` which have `attr_name` unloaded """
        for instance in session:
            if isinstance(instance, mapper.class_):
                state: InstanceState = instance_state(instance)
                # Only return instances that:
                # 1. Are persistent in the DB (have a PK)
                # 2. Have this attribute unloaded
                if state.persistent and attr_name in state.unloaded:
                    yield state


# Raise loader

@ColumnProperty.strategy_for(raiseload_col=True)
@RelationshipProperty.strategy_for(lazy="raiseload_rel")
class RaiseLoader(LoaderStrategy):
    """ Raise Lazy loader """

    def create_row_processor(self, context: QueryContext, path: AbstractEntityRegistry, loadopt: Load, mapper: Mapper,
                             result: ResultProxy, adapter, populators: Mapping[str, list]):
        set_lazy_callable = (
            InstanceState._instance_level_callable_processor
        )(mapper.class_manager, self._raise_lazy_loading, self.key)

        populators["new"].append((self.key, set_lazy_callable))

    def _raise_lazy_loading(self, state: InstanceState, passive='NOT USED'):
        """ Handle the lazy-loading for an attribute on a particular instance

        Args:
            state: The instance whose attribute is being lazy loaded
        """
        raise LazyLoadingAttributeError(
            model_name=self.parent.class_.__name__,
            attribute_name=self.key,
        )
