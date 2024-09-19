# motum
A program to move data using parallel rsync streams


## Usage

Transfer files from local machine to destination server via ssh
```bash
python3 motum.py -n 4 /source/path/folder user@university.it /destination/path
```

If the source directory has a deep tree structure, the option `level` may be used to specify at which inner level the parallelization should happen. For example, in a situation like this
```
source
 - folderA
   - 00
   - 01
   - 02
 - folderB
   - 00
   - 01
   - 02
```
`--level 2` creates multiple parallel streams that copy the folders `00`, `01` etc. while `--level 1` would create only 2 streams copying the folders `folderA` and `folderB`.


Transfer files locally, i.e. w/o using ssh
```bash
python3 motum.py -n 4 /source/path/folder local /destination/path
```