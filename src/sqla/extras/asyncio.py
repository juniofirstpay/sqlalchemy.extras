import inspect
import functools
import logging
from contextvars import ContextVar
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)

async_session_context: ContextVar[AsyncSession] = ContextVar(
    "async_session_context", default=None
)
async_sessionmaker_context: ContextVar[async_sessionmaker] = ContextVar(
    "async_sessionmaker_context", default=None
)


async def _run_callable(
    session: AsyncSession,
    async_callable,
    *args,
    **kwargs
):
    args = (session, *args)

    result = None
    try:
        result = await async_callable(*args, **kwargs)
    except Exception as e:
        logger.error(e, exc_info=1)
        raise e

    return result

def wrapper(async_callable, auto=True, nested=False):
    
    @functools.wraps(async_callable)
    async def session_wrapper(*args, **kwargs):
        session_factory = async_sessionmaker_context.get()
        existing_session = async_session_context.get()

        if nested == True and existing_session is None:
            raise Exception("cannot run nested transaction without session initialized")

        if nested == True and existing_session:
            async with existing_session.begin_nested():
                return await _run_callable(
                    existing_session,
                    async_callable,
                    *args,
                    **kwargs
                )

        if nested == False and existing_session:
            if auto == False:
                return await _run_callable(
                    existing_session,
                    async_callable,
                    *args,
                    **kwargs
                )
            elif auto == True and existing_session.is_active:
                return await _run_callable(
                    existing_session,
                    async_callable,
                    *args,
                    **kwargs
                )
            else:
                async with existing_session.begin():
                    return await _run_callable(
                        existing_session,
                        async_callable,
                        *args,
                        **kwargs
                    )

        async with session_factory() as session:  # type: AsyncSession
            async_session_context.set(session)
            if auto == False:
                return await _run_callable(
                    session,
                    async_callable,
                    *args,
                    **kwargs
                )
            else:
                async with session.begin():
                    try:
                        result = await _run_callable(
                            session,
                            async_callable,
                            *args,
                            **kwargs
                        )
                        await session.commit()
                        return result
                    except Exception as e:
                        await session.rollback()
                        raise e

    return session_wrapper

def with_async_session(*args, **kwargs):
    # this function can either be used as an async generator or decorator for transaction or nested transaction
    # for nested transaction, first parameter to the function should be True boolean value
    if len(args) == 1 and inspect.iscoroutinefunction(args[0]):
        async_callable = args[0]
        nested = False
        return wrapper(async_callable, auto=True, nested=nested)
    else:
        auto = kwargs.get("auto", False)
        nested = kwargs.get("nested", False)
        def delayed_wrapper(async_callable):
            return wrapper(async_callable, auto=auto, nested=nested)
        return delayed_wrapper

    
