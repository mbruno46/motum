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

def bandwidth_init():
    t0 = time.time()
    full = int(os.popen(f'du -sk "{path}"').read().split()[0])
    log(f'total size {full/1024:.2f} MB')
    
    def run():
        if host=='localhost':
            tot = int(os.popen(f'du -sk {dst}').read().split()[0])
        else:
            tot = int(os.popen(f'ssh {host} "du -sk {dst}" 2> /dev/null').read().split()[0])
        if verbose>1:
            log(f'transferred size {tot/1024:.2f} MB')
        dt = time.time() - t0
        sys.stdout.write(f'\r[motum]: effective bandwidth {tot/dt/1024:.2f} MB/sec ; {int(tot/full*100): <3d} % completed')
        sys.stdout.flush()
    
    def inner():
        if not args.dry_run:
            try:
                run()
            except:
                pass
    return inner


class MoveTask:
    def __init__(self, arg):
        self.arg = f'{path}/./{os.path.relpath( arg, path)}'

    def __call__(self):
        cmd = ['rsync','-aczR','-pgot','--partial']
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
            raise subprocess.CalledProcessError(return_code, cmd)            
        

def worker(q):
    while True:
        if q.qsize()>1:
            task = q.get()
            task()
            q.task_done()
        else:
            time.sleep(2)
    
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
      
bandwidth = bandwidth_init()
create_tree(path,level=args.level)

def print_output(bandwidth):
    while True:
        time.sleep(args.timeout)
        bandwidth()
    
Thread(target=print_output, args=(bandwidth,), daemon=True).start()

queue.join()

sys.exit(666)