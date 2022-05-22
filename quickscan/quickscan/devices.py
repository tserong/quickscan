import os
import json
import string
import shutil
import logging

from glob import glob
from typing import Dict, Any, List, Tuple, Optional
from quickscan.common.utils import (
    get_block_devs,
    get_lvm_metadata,
    read_file,
    timeit,
    human_readable_size,
    get_link_data,
    is_device_locked,
    parse_tags)
from quickscan.common.filter import ObjectFilter
from quickscan.common.concurrent import concurrent_cmds, async_run
from quickscan.common.enums import ReportFormat

logger = logging.getLogger(__name__)

# Differences to ceph-volume
# - don't follow partitions - the presence of partitions makes the device ineligible, anyway
# - don't try and get an exclusive lock if the device is already rejected
# - if the device looks empty - inspect the signature with wipefs, to be sure (detects mdraid,
#   btrfs, gpt etc)


class BaseDevice:
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
    _report_template = '{dev:<25} {size:>10}  {rot!s:<7}  {model:<25} {dev_nodes:<16}'
    _report_headings = _report_template.format(
        dev='Device Path',
        size='Size',
        rot='Type',
        model='Model Name',
        dev_nodes='Device Nodes'
    )

    def __init__(self, parent, dev_node: str) -> None:
        self._parent = parent
        self._dev_node = dev_node
        self.dev_path = f'/dev/{dev_node}'
        self.alt_path = ''
        self.mpath_device = ''
        self.mpath_node = ''
        self.dev_nodes = ''
        self.enclosure_id = ''
        self.enclosure_slot = ''
        self.sys_api: Dict[str, Any] = {}
        self.lsm_data: Dict[str, Any] = {}

        self._holders = glob(os.path.join(self._block_dir, self._dev_node, 'holders/*'))
        self.lvs = self._build_lvs()  # must run after _holders is created

        self._build()

    def _build(self) -> None:
        self._process_sysfs()
        self._detect_mpath()

    def _detect_mpath(self) -> None:
        for holder_path in self._holders:
            dev = os.path.basename(holder_path)
            if dev in self._parent._mpath_device_map:
                self.mpath_device = self._parent._mpath_device_map.get(dev)
                self.mpath_node = dev

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
                    for k, v in tags.items():
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
    def scsi_addr(self) -> str:
        scsi_addr_path = glob(f'/sys/block/{self._dev_node}/device/bsg/*')
        return '' if not scsi_addr_path else os.path.basename(scsi_addr_path[0])

    @property
    def device_id(self) -> str:
        vendor = self.sys_api['vendor'].strip() or ''
        model = self.sys_api['model'].strip() or ''
        serial = self.sys_api['serial'].strip() or ''
        return ('_'.join([vendor, model, serial])).replace(' ', '_')

    @property
    def _dev_nodes_str(self):
        return (','.join([os.path.basename(self.dev_path),
                          os.path.basename(self.alt_path)])).rstrip(',')

    @timeit
    def _process_sysfs(self) -> None:
        logger.info(f'processing {self._dev_node}')
        for attrib in self._device_attributes:
            fname = os.path.join(self._block_dir, self._dev_node, attrib)
            content = read_file(fname)
            key = os.path.basename(attrib)

            # post processing
            if key == 'size':
                logical_size = read_file(os.path.join(self._block_dir,
                                                      self._dev_node,
                                                      'queue/logical_block_size'))
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

    def as_text(self) -> str:
        return self._report_template.format(
                dev=self.path,
                size=self.sys_api['human_readable_size'],
                rot='HDD' if self.sys_api['rotational'] == '1' else 'Flash',
                model=self.sys_api['model'],
                dev_nodes=self._dev_nodes_str
            )


class Device(BaseDevice):
    _report_template = '{dev:<25} {size:>10}  {rot!s:<7}  {available!s:<9}  {model:<25} {dev_nodes:<16} {reject}'  # noqa: E501
    _report_headings = _report_template.format(
        dev='Device Path',
        size='Size',
        rot='Type',
        available='Available',
        model='Model Name',
        dev_nodes='Device Nodes',
        reject='Reject Reasons'
    )

    def __init__(self, *args):
        super().__init__(*args)
        self.reject_reasons = []

    @property
    def available(self):
        return False if self.reject_reasons else True

    def analyse(self):
        # Notes
        # 1. maintain this order, since each step can modify the device object attributes
        #    and be referenced in a later check function
        # 2. keep this analysis quick - anything that needs >20ms should be handled in the
        #    parent class analyse phase
        self._check_size()
        self._check_partitions()
        self._check_LVM()
        self._check_locked()

    def _check_size(self) -> None:
        if self.sys_api['size'] < self._min_osd_size_bytes:
            self.reject_reasons.append(
                f'Device too small (< {human_readable_size(self._min_osd_size_bytes)})')

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
            logger.info('skipping "lock" check for mpath enabled device')
        else:
            # device could be free, let's confirm by trying to get an EXCL lock
            if is_device_locked(self.dev_path):
                self.reject_reasons.append('Locked')

    def as_text(self) -> str:
        return self._report_template.format(
                dev=self.path,
                size=self.sys_api['human_readable_size'],
                rot='HDD' if self.sys_api['rotational'] == '1' else 'Flash',
                available=self.available,
                model=self.sys_api['model'],
                dev_nodes=self._dev_nodes_str,
                reject=','.join(self.reject_reasons),
            )


class Devices:

    _dependencies = [
        'lvs',
        'wipefs',
    ]

    def __init__(self, skip_analysis: bool = True, disk_group_size: int = 10) -> None:
        self._skip_analysis = skip_analysis
        self._disk_group_size = disk_group_size
        self._candidate_devices = get_block_devs()
        self._lv_metadata: Dict[str, Dict[str, Any]] = self._build_lv_metadata()
        self._pv_devices: List[str] = [
            tgt for _linkname, tgt in get_link_data('/dev/disk/by-id/lvm-pv-uuid-*')
        ]
        self._mpath_device_map: Dict[str, str] = {
            target: link for link, target in get_link_data('/dev/mapper/mpath*')
        }
        self._lv_device_map = self._build_lv_device_map()
        self._device_data: List[Device] = self._build_devices()

        if not self._skip_analysis:
            self.analyse()

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

    def analyse(self) -> None:

        self._check_signatures()
        self._check_multipath()

    @timeit
    def _check_signatures(self) -> None:
        dev_paths = sorted([dev.path for dev in self._device_data if dev.available])
        if not dev_paths:
            logger.info('all disks are in use, no further analysis needed')
            return

        logger.info(f'inspecting disk signatures for {len(dev_paths)} devices: {dev_paths}')

        # Split the disks we need to take a closer look at into groups, then pass to
        # asyncio to get the signatures
        disk_groups = []
        for i in range(0, len(dev_paths), self._disk_group_size):
            paths = ' '.join(dev_paths[i:i + self._disk_group_size])
            disk_groups.append(f'wipefs -J --noheadings {paths}')

        logger.debug(f'starting {len(disk_groups)} concurrent signature checks')
        data = async_run(concurrent_cmds(disk_groups))
        logger.debug('finished concurrent command execution')

        for completion in data:
            if completion.returncode != 0:
                logger.error(f'wipefs command failed for {" ".join(completion.args)}')
                continue

            if completion.stdout:
                js = json.loads(completion.stdout.decode('utf-8'))
                signatures = {}
                for sig in js.get('signatures'):
                    dev_name = sig['device']
                    if dev_name in signatures:
                        signatures[dev_name].add(sig['type'])
                    else:
                        signatures[dev_name] = {sig['type']}

                for dev in self._device_data:
                    if not dev.mpath_device:
                        check_name = dev._dev_node
                    else:
                        check_name = os.path.basename(dev.mpath_device)

                    if check_name in signatures:
                        logger.info(f'disk signature detected - rejecting {check_name}')
                        dev.reject_reasons.append(f'{",".join(signatures[check_name])} detected')

        logger.info('finished')

    @timeit
    def _check_multipath(self) -> None:
        for dev in self._device_data:
            if dev.alt_path and not dev.mpath_node:
                dev.reject_reasons.append('multipath configuration missing')

    @timeit
    def _build_devices(self) -> List[Device]:
        dev_list = []
        dev_map = {}
        for dev_node in self._candidate_devices:
            if self._skip_analysis:
                dev = BaseDevice(self, dev_node)
            else:
                dev = Device(self, dev_node)

            if dev.device_id in dev_map:
                existing_device = dev_map[dev.device_id]
                existing_device.alt_path = f'/dev/{dev_node}'
                logger.info(f'skipping {dev_node} as a duplicate of {existing_device.dev_path}')
            else:
                if not self._skip_analysis:
                    dev.analyse()
                dev_map[dev.device_id] = dev
                dev_list.append(dev)

        return dev_list

    def as_json(self, dev_filter: Optional[ObjectFilter] = None) -> str:
        s = ''
        for dev in self._device_data:
            if dev_filter and not dev_filter.ok(dev):
                continue
            s += f'{dev.as_json()},\n'
        return f'[{s}]' if s else '[]'

    def as_text(self, dev_filter: Optional[ObjectFilter] = None) -> str:
        if self._skip_analysis:
            headings = BaseDevice._report_headings
        else:
            headings = Device._report_headings

        output = [headings]

        for dev in sorted(self._device_data, key=lambda dev: dev.path):
            if dev_filter and not dev_filter.ok(dev):
                continue
            output.append(dev.as_text())

        if len(output) == 1:
            sfx = ' that match your filter' if dev_filter else ''
            return f'No devices found{sfx}'
        else:
            dev_count = len(output) - 1
            output.append(f'{dev_count} devices listed')

        return '\n'.join(output)

    def report(self, mode: ReportFormat = 'text', dev_filter: Optional[ObjectFilter] = None) -> str:
        if mode == 'json':
            return self.as_json(dev_filter)
        return self.as_text(dev_filter)
