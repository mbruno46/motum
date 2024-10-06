#!/usr/bin/python3

import sys
import time
import os
import subprocess

from queue import Queue
from threading import Thread
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("src")
parser.add_argument("host")
parser.add_argument("dst")
parser.add_argument("-n","--parallel-streams",default=1,type=int)
parser.add_argument("--timeout",default=1,type=int)
parser.add_argument("-v","--verbose",default=0,type=int)
parser.add_argument("--level",default=1,type=int)
parser.add_argument("--dry-run",action='store_true')
parser.add_argument("--checksum",action='store_true')

args = parser.parse_args()

path = os.path.abspath(args.src)
host = args.host

class Log:
    def __init__(self):
        self.t0 = time.time()

    def __call__(self, msg):
        dt = time.time() - self.t0
        print(f'[motum {dt:.4e} s]: {msg}', flush=True)

log = Log()

def run_cmd(cmd, remote, parser = lambda x: x.read().rstrip()):
    runner = lambda c: parser(os.popen(c))
    if remote and (host!='localhost'):
        return runner(f'ssh {host} "{cmd}" 2> /dev/null')
    return runner(cmd)
    
def getsize(p, r):
    return run_cmd(f'du -sb {p}', remote = r, parser = lambda x: int(x.read().split()[0]))

# checks if destination path exists
if run_cmd(f'if [ -d {args.dst} ]; then echo "Y"; fi', remote=True)!='Y':
    log('Folder {args.dst} does not exists on host')
    sys.exit(1)

# creates proper dst variable
_dst = run_cmd(f'echo {args.dst}', True)
dst = os.path.join(_dst, os.path.basename(path) + '/').replace(' ','\ ')

log(f'{path} -> {host}:{dst}')

verbose = args.verbose

class Bandwidth:
    def __init__(self):
        self.t0 = time.time()
        self.s0 = 0
        self.full = getsize(path, False)
        log(f'total size {self.full/1024**2:.2f} MB')

    def run(self):
        s1 = getsize(dst, True)
        t1 = time.time()
        dt = t1 - self.t0
        ds = s1 - self.s0
        self.t0, self.s0 = t1, s1
        if verbose>1:
            log(f'transferred size {ds/1024**2:.2f} MB')
        sys.stdout.write(f'\r[motum]: effective bandwidth {ds/dt/1024**2:.2f} MB/sec ; {int(s1/self.full*100): <3d} % completed')
        sys.stdout.flush()
    
    def __call__(self):
        if not args.dry_run:
            try:
                self.run()
            except:
                pass

def create_rsync_cmd(arg):
    cmd = ['rsync','-aczR','-pgot','--partial','--inplace']
    if args.dry_run:
        cmd += ['--dry-run']
    cmd += [arg]

    if host=='localhost':
        cmd += [dst]
    else:
        cmd += ['-e','ssh',f'{host}:{dst}']
    return cmd

class MoveTask:
    def __init__(self, arg):
        self.arg = f'{path}/./{os.path.relpath( arg, path)}'

    def __call__(self):
        cmd = create_rsync_cmd(self.arg)
        if verbose>0:
            log(f'starting transfer of {self.arg}')
            log(f'{" ".join(cmd)}')

        t0 = time.time()
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        p.stdout.close()
        return_code = p.wait()
        if return_code:
            log(f"ERROR {cmd}\n{return_code}")
            # raise subprocess.CalledProcessError(return_code, cmd)
        
class CheckSumTask:
    def __init__(self, f1, f2):
        self.f1 = f1
        self.f2 = f2

    def __call__(self):
        m1 = run_cmd(f'md5sum {self.f1}', remote=False).split()[0]
        m2 = run_cmd(f'md5sum {self.f2}', remote=True).split()[0]
        if m2==m1:
            log(f'{self.f1} OK')
        else:
            log(f'{self.f1} ERROR')

def worker(q):
    while True:
        task = q.get()
        task()
        q.task_done()
    
queue = Queue()

for t in [Thread(target=worker, args=(queue,), daemon=True) for _ in range(args.parallel_streams)]:
    t.start()
    
def create_tree(path, level=1):    
    with os.scandir(path) as it:
        for entry in it:
            if entry.name.startswith('.'):
                continue
                
            workdir = os.path.join(path,entry.name)
            if (level>1) and (os.path.isdir(workdir)):
                create_tree(workdir, level-1)
            else:
                queue.put(MoveTask(workdir))

def create_tree_checksum(p):    
    with os.scandir(p) as it:
        for entry in it:
            if entry.name.startswith('.'):
                continue
            
            work = os.path.join(p, entry.name)
            dest = os.path.join(dst, os.path.relpath(work, path))
            if os.path.isdir(work):
                create_tree_checksum(work)
            else:
                queue.put(CheckSumTask(work, dest))


if args.checksum:
    ds = 0
    for _s, _p, _r in zip([+1,-1],[path,dst],[False,True]):
        size = getsize(_p, _r)
        log(f'{size} size of {_p}')
        ds = ds + _s * size
    if ds!=0.0:
        log(f'WARNING sizes do not match!')

    # cmd = "find . -not -path '*/.*' -type f"
    cmd = "find . -type f"
    lf = [run_cmd(f'cd {_p}; {cmd}', remote=_r, parser = lambda x: x.readlines()) for _p, _r in zip([path,dst],[False,True])]
    not_copied = []
    partial = []
    for f in lf[0]:
        if not f in lf[1]:
            not_copied += [f.rstrip()]
        #else:
        #    s1 = getsize(os.path.join(path,f),False)
        #    s2 = getsize(os.path.join(dst,f),True)
        #    if abs(s1-s2)>8:
        #        log(f"WARNING: sizes of file {f} do not match = {s1} vs {s2}")

    if not_copied:
        for f in not_copied:
            log(f"Missing file {f}")
            if input("Do you want to copy it? [y/n]")=='y':
                MoveTask(path+'/'+f)()

    create_tree_checksum(path)
else:
    bandwidth = Bandwidth()
    create_tree(path,level=args.level)

    def print_output(bandwidth):
        while True:
            time.sleep(args.timeout)
            bandwidth()
        
    Thread(target=print_output, args=(bandwidth,), daemon=True).start()

queue.join()

sys.exit(666)
