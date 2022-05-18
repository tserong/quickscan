import sys
import time
import argparse

from quickscan import Devices
from quickscan.common.enums import ReportFormat,LogLevel
import logging

# set up the logger
logging.basicConfig(
    filename='quickscan.log',
    filemode='w',
    format='%(asctime)s [%(levelname)-7s] : %(name)s : %(funcName)s : %(message)s')


def main(args: argparse.Namespace) -> None:
    
    ok_to_run, reasons = Devices.can_run()
    if not ok_to_run:
        print('Error: Unable to start')
        print('\n'.join(reasons))
        sys.exit(4)

    logging.info('Starting...')
    start_time = time.time()
    devices = Devices()
    logging.info(f'Completed, runtime: {(time.time() - start_time):.6f}s')
    print(devices.report(mode=args.format.value))

def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--format',
        default='json', 
        type=ReportFormat,
        choices=list(ReportFormat),
        help='report format for the disk inventory')

    parser.add_argument(
        '--loglevel',
        default='debug', 
        type=LogLevel,
        choices=list(LogLevel),
        help='log level')
    
    return parser.parse_args()


if __name__ == '__main__':

    args = get_args()

    logger = logging.getLogger()
    level = logging.getLevelName(str(args.loglevel).upper())
    logger.setLevel(level)

    main(args)
