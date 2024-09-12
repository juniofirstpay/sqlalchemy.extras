import inspect
import functools
from contextvars import ContextVar
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

async_session_context: ContextVar[AsyncSession] = ContextVar(
    "async_session_context", default=None
)
async_sessionmaker_context: ContextVar[async_sessionmaker] = ContextVar(
    "async_sessionmaker_context", default=None
)


def _session_wrap(session_factory: async_sessionmaker, async_callable, is_nested=False):
    @functools.wraps(async_callable)
    async def session_wrapper(*args, **kwargs):
        existing_session = async_session_context.get()

        if is_nested == True and existing_session is None:
            raise Exception("Parent session not started")

        if is_nested == True and existing_session:
            async with existing_session.begin_nested() as nested:
                try:
                    result = await async_callable(*args, **kwargs)
                    nested.commit()
                    return result
                except Exception as e:
                    nested.rollback()

        if existing_session:
            async with existing_session:
                return await async_callable(*args, **kwargs)

        async with session_factory() as session:
            async with session:
                async_session_context.set(session)
                return await async_callable(*args, **kwargs)

    return session_wrapper


def with_async_session(*args):
    # this function can either be used as an async generator or decorator for transaction or nested transaction
    # for nested transaction, first parameter to the function should be True boolean value

    session_factory = async_sessionmaker_context.get()
    if len(args) > 0:
        async_callable = None
        nested_transaction_flag = False

        if inspect.iscoroutinefunction(args[0]):
            async_callable = args[0]

        if async_callable is not None:
            return _session_wrap(session_factory, async_callable)

        if isinstance(args[0], bool):
            nested_transaction_flag = args[0]

        if len(args) == 1:

            def session_wrap(async_callable):
                return _session_wrap(
                    session_factory, async_callable, nested=nested_transaction_flag
                )

            return session_wrap

        return session_wrap(
            session_factory, async_callable, nested=nested_transaction_flag
        )
