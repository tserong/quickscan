import os
import time
import json
import subprocess
from glob import glob
from pathlib import Path

from .defaults import excluded_block_devices
from typing import List, Dict, Any, Tuple
import logging
logger = logging.getLogger(__name__)

def timeit(func):
    def wrap(*args, **kwargs):
        start_time = time.time()
        logger.debug(f'{func.__name__} starting')
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        logger.debug(f'{func.__name__} complete. Runtime {elapsed:.10f} secs')
        return result
    return wrap


@timeit
def get_block_devs() -> List[str]:
    """Determine the list of block devices by looking at /sys/block"""
    devs = [dev for dev in os.listdir('/sys/block')
            if not dev.startswith(excluded_block_devices)]
    logger.info(f'{len(devs)} devices detected')
    return devs


def issue_cmd(cmd: str) -> subprocess.CompletedProcess:
    return subprocess.run(cmd.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def parse_tags(tags_str:str) -> Dict[str, str]:
    tags = {}
    for tag in tags_str.split(','):
        k, v = tag.split('=')
        tags[k] = v
    return tags

@timeit
def get_lvm_metadata(lvm_cmd: str, lvm_type: str) -> List[Dict[str, Any]]:
    """return all LVM metadata on the host"""
    response = issue_cmd(lvm_cmd)
    if response.returncode == 0:
        js = json.loads(response.stdout.decode('utf-8'))
        try:
            data = js['report'][0][lvm_type]
            logger.info(f'lvm call returned entries for {len(data)} objects')
            return data
        except KeyError:
            logger.warning('Unable to marshall lvm output into JSON - format changed?')
            return []

    cmd_name = lvm_cmd.split(' ')[0]
    logger.warning(f'LVM {cmd_name} command failed with rc={response.returncode}: {str(response.stderr)}')
    return []


def get_link_data(glob_pattern) -> List[Tuple[str, str]]:
    return [(link, os.path.basename(str(Path(link).resolve()))) for link in glob(glob_pattern)]

def read_file(file_name) -> str:
    if os.path.exists(file_name):
        with open(file_name, 'rb') as f:
            try:
                return f.read().decode('utf-8', 'ignore').strip()
            except OSError as e:
                logger.error(f'Error reading {file_name}')
                return ''
    else:
        return 'unknown'


def human_readable_size(size):
    """
    Take a size in bytes, and transform it into a human readable size with up
    to two decimals of precision.
    """
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    for suffix in suffixes:
        if size >= 1024:
            size = size / 1024
        else:
            break
    return "{size:.2f} {suffix}".format(
        size=size,
        suffix=suffix)

@timeit
def is_device_locked(path: str) -> bool:
    open_flags = (os.O_RDWR | os.O_EXCL)
    open_mode = 0
    fd = None
    logger.info(f'checking {path} is not locked')
    try:
        fd = os.open(path, open_flags, open_mode)
    except OSError:
        return True

    try:
        os.close(fd)
    except OSError:
        return True

    return False
