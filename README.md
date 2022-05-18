# quickscan
Experiment in disk scanning..maybe to help ceph-volume

## Context
The runtime for a ceph-volume inventory is proportional to the number of devices on the host. Whilst this seems reasonable and large hosts this can equate to over a minute before the inventory subcommand returns to the user. The current ceph-volume code uses shell commands extensively to determine the disk configuration - and processes eligible disks in a sequential manner.

This repo is just an experiment to examine whether an alternative approach to inventory can yield faster response times, whilst delivering the same content back to the caller.

## Testing
To test the code on your system, grab the archive and extract it on to your host machine.

Then run the scan from the root of this directory (no installation needed)
```
# sudo python3 ./quickscan.py --format text
```

This will show you something like this;
```
paul@disktests:~/quickscan$ time sudo python3 ./quickscan.py --format text 
Device Path                     Size  Rotates  Available  Model name                Device Nodes     Reject Reasons
/dev/mapper/mpathb          30.00 GB  True     False      vdisk                     sdd,sda          LVM device
/dev/mapper/mpathc          15.00 GB  True     False      vdisk                     sdb,sde          PMBR detected
/dev/mapper/mpathd          15.00 GB  True     True       vdisk                     sdf,sdc          
/dev/sdg                    50.00 GB  True     True       QEMU HARDDISK             sdg              
/dev/sdh                    50.00 GB  True     False      QEMU HARDDISK             sdh              btrfs detected
/dev/sdi                    50.00 GB  True     False      QEMU HARDDISK             sdi              Has partitions
/dev/sdj                    50.00 GB  True     False      QEMU HARDDISK             sdj              LVM device
/dev/sdk                    50.00 GB  True     False      QEMU HARDDISK             sdk              LVM device
/dev/sdl                     4.00 GB  True     False      QEMU HARDDISK             sdl              Device too small (< 10.00 GB)
/dev/sdm                    30.00 GB  True     False      QEMU HARDDISK             sdm              Has partitions
/dev/sdn                    50.00 GB  True     False      QEMU HARDDISK             sdn              Locked

real    0m0.282s
user    0m0.134s
sys     0m0.063s
```

When quickscan runs, it will create/overwrite a log file `quickscan.log` - you can use this to track the performance and behaviour of the analysis.

A `--format` parameter supports json, and there is also a `--loglevel` parameter to tweak the contents of the log (the default is debug, which includes method timings)

You can also see that the code has multipath support and shows only one device, but two device nodes. 

