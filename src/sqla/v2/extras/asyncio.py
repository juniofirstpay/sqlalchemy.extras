import functools
from contextvars import ContextVar
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from collections.abc import Callable, Coroutine
from typing import Concatenate, TypeVar, ParamSpec
import structlog

logger = structlog.get_logger(__name__)


T = TypeVar("T")
P = ParamSpec("P")


class WithDbSession:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.__db_session_factory = session_factory
        self.__db_session_ctx: ContextVar[AsyncSession] = ContextVar(
            "WithDbSession__db_session_ctx", default=None
        )

    def __call__(
        self,
        func: Callable[Concatenate[AsyncSession, P], Coroutine[None, None, T]],
    ) -> Callable[P, Coroutine[None, None, T]]:

        @functools.wraps(func)
        async def wrapped_func(*args: P.args, **kwargs: P.kwargs) -> T:
            if session := self.__db_session_ctx.get():
                # await log.adebug("found-existing-session for %r", func)
                return await func(session, *args, **kwargs)

            async with self.__db_session_factory() as session:
                # await log.adebug("creating new session for %r", func)
                prev_session_token = self.__db_session_ctx.set(session)
                try:
                    result = await func(session, *args, **kwargs)
                    await session.commit()
                except Exception as e:
                    await logger.aerror(e)
                    await session.rollback()
                    raise e
                finally:
                    self.__db_session_ctx.reset(prev_session_token)
                return result

        return wrapped_func


__all__ = ("WithDBSession",)
