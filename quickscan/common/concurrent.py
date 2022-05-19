import asyncio

from subprocess import CompletedProcess
from .utils import issue_cmd
from typing import List
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


async def run_cmd(cmd: str) -> CompletedProcess:
    logger.debug(f'running command {cmd}')
    response = issue_cmd(cmd)
    return response


async def concurrent_cmds(cmd_list: List[str]) -> List[CompletedProcess]:
    tasks = []
    for cmd in cmd_list:
        tasks.append(run_cmd(cmd))
    
    data = await asyncio.gather(*tasks)

    return data
