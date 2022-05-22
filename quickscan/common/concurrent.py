import asyncio

from subprocess import CompletedProcess
from typing import List, Callable
import logging


logger = logging.getLogger(__name__)


try:
    from asyncio import run as async_run   # type: ignore[attr-defined]
except ImportError:
    def async_run(coro):  # type: ignore
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                asyncio.set_event_loop(None)
                loop.close()


async def run_func(func: Callable, cmd: str) -> CompletedProcess:
    logger.debug(f'running function {func.__name__}, with parms: {cmd}')
    response = func(cmd)
    return response


async def concurrent_cmds(func: Callable, cmd_list: List[str]) -> List[CompletedProcess]:
    tasks = []
    for cmd in cmd_list:
        tasks.append(run_func(func, cmd))

    data = await asyncio.gather(*tasks)

    return data
