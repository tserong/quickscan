# quickscan
Experiment in disk scanning..maybe to help ceph-volume

## Context
The runtime for a ceph-volume inventory is proportional to the number of devices on the host. Whilst this seems reasonable and large hosts this can equate to over a minute before the inventory subcommand returns to the user. The current ceph-volume code uses shell commands extensively to determine the disk configuration - and processes eligible disks in a sequential manner.

This repo is just an experiement to examine whether an alternative approach to inventory can yield faster response times, whilst delivering the same content back to the caller.
