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

if host=='localhost':
    dst = os.path.join(args.dst, os.path.basename(path) + '/').replace(' ','\ ')
else:
    _dst = os.popen(f'ssh {host} "echo {args.dst}" 2> /dev/null').read().strip()
    dst = os.path.join(_dst, os.path.basename(path) + '/').replace(' ','\ ')

verbose = args.verbose

class Log:
    def __init__(self):
        self.t0 = time.time()

    def __call__(self, msg):
        dt = time.time() - self.t0
        print(f'[motum {dt:.4e} s]: {msg}', flush=True)

log = Log()
log(f'{path} -> {host}:{dst}')

def run_cmd(cmd, remote, parser = lambda x: x.read().rstrip()):
    runner = lambda c: parser(os.popen(c))
    if remote and (host!='localhost'):
        return runner(f'ssh {host} "du -sk {dst}" 2> /dev/null')
    return runner(cmd)
    
class Bandwidth:
    def __init__(self):
        self.t0 = time.time()
        self.s0 = 0
        self.full = int(os.popen(f'du -sk "{path}"').read().split()[0])
        log(f'total size {self.full/1024:.2f} MB')

    def run(self):
        if host=='localhost':
            s1 = int(os.popen(f'du -sk {dst}').read().split()[0])
        else:
            s1 = int(os.popen(f'ssh {host} "du -sk {dst}" 2> /dev/null').read().split()[0])
        t1 = time.time()
        dt = t1 - self.t0
        ds = s1 - self.s0
        self.t0, self.s0 = t1, s1
        if verbose>1:
            log(f'transferred size {ds/1024:.2f} MB')
        sys.stdout.write(f'\r[motum]: effective bandwidth {ds/dt/1024:.2f} MB/sec ; {int(s1/self.full*100): <3d} % completed')
        sys.stdout.flush()
    
    def __call__(self):
        if not args.dry_run:
            try:
                self.run()
            except:
                pass

# def bandwidth_init():
#     t0 = time.time()
#     tot0 = 0
#     full = int(os.popen(f'du -sk "{path}"').read().split()[0])
#     log(f'total size {full/1024:.2f} MB')
    
#     def run():
#         if host=='localhost':
#             tot = int(os.popen(f'du -sk {dst}').read().split()[0])
#         else:
#             tot = int(os.popen(f'ssh {host} "du -sk {dst}" 2> /dev/null').read().split()[0])
#         if verbose>1:
#             log(f'transferred size {tot/1024:.2f} MB')
#         t1 = time.time()
#         dt = t1 - t0
#         ds = tot - tot0
#         t0 = t1
#         tot0 = tot
#         sys.stdout.write(f'\r[motum]: effective bandwidth {ds/dt/1024:.2f} MB/sec ; {int(tot/full*100): <3d} % completed')
#         sys.stdout.flush()
    
#     def inner():
#         if not args.dry_run:
#             run()
#             # try:
#                 # run()
#             # except:
#                 # pass
#     return inner

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
        cmd = ['rsync','-aczR','-pgot','--partial','--inplace']
        if args.dry_run:
            cmd += ['--dry-run']
        cmd += [self.arg]

        if host=='localhost':
            cmd += [dst]
        else:
            cmd += ['-e','ssh',f'{host}:{dst}']

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
        size = int(run_cmd(f'du -sk {_p}', remote=_r).split()[0])
        log(f'{size} size of {_p}')
        ds = ds + _s * size
    if ds!=0.0:
        log(f'WARNING sizes do not match!')

    # cmd = "find . -not -path '*/.*' -type f"
    cmd = "find . -type f"
    lf = [run_cmd(f'cd {_p}; {cmd}', remote=_r, parser = lambda x: x.readlines()) for _p, _r in zip([path,dst],[False,True])]
    not_copied = []
    for f in lf[0]:
        if not f in lf[1]:
            not_copied += [f.rstrip()]
    if not_copied:
        for f in not_copied:
            log(f"Missing file {f}")
            if input("Do you want to copy it? [y/n]")=='y':
                MoveTask(path+'/'+f)()
        # sys.exit(999)
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