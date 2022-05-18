import os
import json
from re import S
import string
import shutil
import logging

from glob import glob
from typing import Dict, Any, List, Tuple
from quickscan.common.utils import (
    get_block_devs,
    get_lvm_metadata,
    issue_cmd,
    read_file,
    timeit,
    human_readable_size,
    get_link_data,
    is_device_locked,
    parse_tags)
from quickscan.common.enums import ReportFormat

logger = logging.getLogger(__name__)

# Differences to ceph-volume
# - don't follow partitions - the presence of partitions makes the device ineligible, anyway
# - don't try and get an exclusive lock if the device is already rejected
# - if the device looks empty - inspect the signature with wipefs, to be sure (detects mdraid, btrfs etc)


class Device:
    # FIXME - make this a tuple of (src_name, stored_name)
    _device_attributes = [
        'removable', 
        'size', 
        'ro',
        'device/model',
        'device/vendor',
        'device/wwid',
        'device/vpd_pg80',
        'device/rev',
        'queue/nr_requests',
        'queue/rotational',
        'queue/scheduler',
        'queue/discard_granularity',
    ]
    _block_dir = '/sys/block'
    _min_osd_size_bytes = 10737418240

    def __init__(self, parent, dev_node: str) -> None:
        self._parent = parent
        self._dev_node = dev_node
        self.dev_path = f'/dev/{dev_node}'
        self.alt_path = ''
        self.mpath_device = ''
        self.mpath_node = ''
        self.sys_api: Dict[str, Any] = {}
        self.lsm_data: Dict[str, Any] = {}
        self.reject_reasons: List[str] = []

        self._holders = glob(os.path.join(self._block_dir, self._dev_node, 'holders/*'))
        self.lvs = self._build_lvs()  # must run after _holders is created
        
        self._process_sysfs()
   
    def analyse(self) -> None:

        # maintain this order, since each step can modify the device object attributes
        # and be referenced in a later check function
        self._check_mpath()
        self._check_size()
        self._check_partitions()
        self._check_LVM()
        self._check_locked()

    def _check_mpath(self) -> None:
        for holder_path in self._holders:
            dev = os.path.basename(holder_path)
            if dev in self._parent._mpath_device_map:
                self.mpath_device = self._parent._mpath_device_map.get(dev)
                self.mpath_node = dev

    def _check_size(self) -> None:
        if self.sys_api['size'] < self._min_osd_size_bytes:
            self.reject_reasons.append(f'Device too small (< {human_readable_size(self._min_osd_size_bytes)})')

    def _check_partitions(self) -> None:
        dirs = glob(os.path.join(self._block_dir, self._dev_node, f'{self._dev_node}*'))
        if dirs:
            self.reject_reasons.append('Has partitions')

    def _check_LVM(self) -> None:
        dev_nodes = f'{self._dev_node} {self.mpath_node}'.split(' ')
        if any(node in self._parent._pv_devices for node in dev_nodes):
            self.reject_reasons.append('LVM device')

    def _check_locked(self) -> None:
        if self.reject_reasons:
            logger.info(f'skipping "lock" check, since {self.dev_path} has already been rejected')
        elif self.mpath_device:
            logger.info(f'skipping "lock" check for mpath enabled device')
        else:
            # device could be free, let's confirm by trying to get an EXCL lock
            if is_device_locked(self.dev_path):
                self.reject_reasons.append('Locked')

    def _build_lvs(self) -> List[Dict[str, Any]]:
        lvs = []
        for holder_path in self._holders:
            holder_dev = os.path.basename(holder_path)
            lv_info = self._parent._lv_device_map.get(holder_dev, {})
            if lv_info:
                key = f'{lv_info["vg_name"]}-{lv_info["lv_name"]}'
                metadata = self._parent._lv_metadata.get(key, {})
                tags_str = metadata.get('lv_tags', '')
                ceph_lv = False
                if tags_str:
                    tags = parse_tags(tags_str)
                    for k,v in tags.items():
                        if k.startswith('ceph.'):
                            ceph_lv = True
                            k = k[5:]
                        lv_info[k] = v
                if not ceph_lv:
                    lv_info['comment'] = 'not used by ceph'

                lvs.append(lv_info)
        return lvs

    @property
    def path(self) -> str:
        return self.mpath_device if self.mpath_device else self.dev_path

    @property
    def available(self) -> bool:
        return False if self.reject_reasons else True

    @property
    def device_id(self) -> str:
        vendor = self.sys_api['vendor'].strip() or ''
        model = self.sys_api['model'].strip() or ''
        serial = self.sys_api['serial'].strip() or ''
        return ('_'.join([vendor, model, serial])).replace(' ', '_')

    @timeit
    def _process_sysfs(self) -> None:
        logger.info(f'processing {self._dev_node}')
        for attrib in self._device_attributes:
            fname = os.path.join(self._block_dir, self._dev_node, attrib)
            content = read_file(fname)
            key = os.path.basename(attrib)

            # post processing
            if key == 'size':
                logical_size = read_file(os.path.join(self._block_dir, self._dev_node, 'queue/logical_block_size'))
                self.sys_api['sectors'] = int(content)
                self.sys_api['sectorsize'] = int(logical_size)
                try:
                    content = int(logical_size) * int(content)
                except ValueError:
                    content = 0
                self.sys_api['human_readable_size'] = human_readable_size(content)

            elif key == 'scheduler':
                active_scheduler = [opt[1:-1] for opt in content.split(' ') if opt.startswith('[')]
                if active_scheduler:
                    content = active_scheduler[0]

            elif key == 'vpd_pg80':
                key = 'serial'
                content = ''.join([ch for ch in content if ch in string.printable])

            self.sys_api[key] = content

    def as_json(self) -> str:
        data = {
            k: getattr(self, k) for k in dir(self)
            if not k.startswith('_')
            and isinstance(getattr(self, k), (float, int, str, list, dict, tuple))
        }
        return json.dumps(data, indent=2, sort_keys=True)
        

class Devices:

    report_template = '{dev:<25} {size:>10}  {rot!s:<7}  {available!s:<9}  {model:<25} {dev_nodes:<16} {reject}'
    _dependencies = [
        'lvs',
        'wipefs',
    ]

    def __init__(self) -> None:
        self._candidate_devices = get_block_devs()
        self._lv_metadata: Dict[str, Dict[str, Any]] = self._build_lv_metadata() 
        self._pv_devices: List[str] = [tgt for _linkname, tgt in get_link_data('/dev/disk/by-id/lvm-pv-uuid-*')]
        self._mpath_device_map: Dict[str, str] = {target: link for link, target in get_link_data('/dev/mapper/mpath*')}
        self._lv_device_map = self._build_lv_device_map()
        self._device_data: List[Device] = self._build_devices()

        self._analyse()

    @classmethod
    def can_run(cls) -> Tuple[bool, List[str]]:
        reasons = []

        if os.getuid() != 0:
            reasons.append('must be root or run with sudo privileges')
        if not os.path.exists('/dev/disk'):
            reasons.append('/dev/disk not present - udev required')
        for pgm in cls._dependencies:
            if not shutil.which(pgm):
                reasons.append(f'{pgm} not installed')

        return len(reasons) == 0, reasons

    def _build_lv_metadata(self) -> Dict[str, Dict[str, Any]]:
        lv_metadata = {}
        raw_lv_metadata = get_lvm_metadata('lvs -a -o vgname,lvname,tags --reportformat=json', 'lv')
        for lv in raw_lv_metadata:
            key = f'{lv["vg_name"]}-{lv["lv_name"]}'
            lv_metadata[key] = lv
        return lv_metadata

    def _build_lv_device_map(self) -> Dict[str, Dict[str, str]]:
        map = {}
        for link_name, target in get_link_data('/dev/disk/by-id/dm-name-*'):
            link = os.path.basename(link_name.replace('--', '*'))
            components = link.split('-')
            if len(components) != 4:
                logger.debug(f'skipping linkname {link_name}. Not an LV link')
            else:
                map[target] = {
                    'vg_name': components[2].replace('*', '-'),
                    'lv_name': components[3].replace('*', '-')
                }
        return map

    @timeit
    def _analyse(self):

        dev_paths = sorted([dev.path for dev in self._device_data if dev.available])
        if not dev_paths:
            logger.info('all disks are in use')
            return
        
        logger.info(f'inspecting disk signatures for {len(dev_paths)} devices: {dev_paths}')

        # use wipefs to reveal disk signatures for disks that look empty
        response = issue_cmd(f'wipefs -J --noheadings {" ".join(dev_paths)}')
        if response.returncode == 0:
            if response.stdout:
                js = json.loads(response.stdout.decode('utf-8'))
                signatures = { sig['device']: sig for sig in js.get('signatures', [])}

                for dev in self._device_data:
                    check_name = dev._dev_node if not dev.mpath_device else os.path.basename(dev.mpath_device)
                    if check_name in signatures:
                        logger.info(f'rejecting {check_name}')
                        dev.reject_reasons.append(f'{signatures[check_name].get("type")} detected')
            else:
                logger.info('no signatures found')
        else:
            # FIXME
            logger.error(f'inspect failed RC={response.returncode}: {response.stderr}')
        
        logger.info('finished')

    @timeit
    def _build_devices(self) -> List[Device]:
        dev_list = []
        dev_map = {}
        for dev_node in self._candidate_devices:
            dev = Device(self, dev_node)
            if dev.device_id in dev_map:
                existing_device = dev_map[dev.device_id]
                existing_device.alt_path = f'/dev/{dev_node}'
                logger.info(f'skipping {dev_node} as a duplicate of {existing_device.dev_path}')
            else:
                dev.analyse()
                dev_map[dev.device_id] = dev
                dev_list.append(dev)

        return dev_list

    def as_json(self) -> str:
        s = ''
        for dev in self._device_data:
            s += dev.as_json()
        return s
    
    def as_text(self) -> str:
        output = [
            self.report_template.format(
                dev='Device Path',
                size='Size',
                rot='Rotates',
                model='Model name',
                available='Available',
                dev_nodes='Device Nodes',
                reject='Reject Reasons'
            )]

        for device in sorted(self._device_data, key=lambda dev: dev.path):
            device_nodes = ','.join([os.path.basename(device.dev_path), os.path.basename(device.alt_path)])
            output.append(
                self.report_template.format(
                    dev=device.path,
                    size=device.sys_api['human_readable_size'],
                    rot=True if device.sys_api['rotational'] == '1' else False,
                    model=device.sys_api['model'],
                    available=device.available,
                    dev_nodes=device_nodes.rstrip(','),
                    reject=','.join(device.reject_reasons),
                )
            )
        return '\n'.join(output)

    def report(self, mode: ReportFormat='text') -> str:
        if mode == 'json':
            return self.as_json()
        return self.as_text()
