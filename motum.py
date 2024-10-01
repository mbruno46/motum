#!/usr/bin/python3

import sys
import time
import os
import subprocess
import re

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

args = parser.parse_args()

path = os.path.abspath(args.src)
host = args.host

if host=='localhost':
    dst = os.path.join(args.dst, os.path.basename(path) + '/').replace(' ','\ ')
else:
    _dst = os.popen(f'ssh {host} "echo {args.dst}" 2> /dev/null').read().strip()
    dst = os.path.join(_dst, os.path.basename(path) + '/').replace(' ','\ ')

verbose = args.verbose

print(f'[motum]: {path} -> {host}:{dst}')

def bandwidth_init():
    t0 = time.time()
    full = int(os.popen(f'du -sk "{path}"').read().split()[0])
    print(f'[motum]: total size {full/1024:.2f} MB')
    
    def inner():
        try:
            if host=='localhost':
                tot = int(os.popen(f'du -sk {dst}').read().split()[0])
            else:
                tot = int(os.popen(f'ssh {host} "du -sk {dst}" 2> /dev/null').read().split()[0])
            if verbose>1:
                print(f'[motum]: transferred size {tot/1024:.2f} MB')
            dt = time.time() - t0
            sys.stdout.write(f'\r[motum]: effective bandwidth {tot/dt/1024:.2f} MB/sec ; {int(tot/full*100): <3d} % completed')
            sys.stdout.flush()
        except:
            pass
    return inner


class MoveTask:
    def __init__(self, arg):
        self.arg = arg
        
    def __call__(self):
        if host=='localhost':
            cmd = ['rsync','-acz','-pgot',self.arg,dst]
        else:
            cmd = ['rsync','-acz','-pgot',self.arg,'-e','ssh',f'{host}:{dst}']
        if verbose>0:
            print(f'[motum]: starting transfer of {self.arg}')
            print(f'[motum]: {" ".join(cmd)}')
        t0 = time.time()
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        p.stdout.close()
        return_code = p.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, cmd)            
        
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
                
            if level>1:
                create_tree(os.path.join(path,entry.name), level-1)
            else:
                queue.put(MoveTask(os.path.join(path,entry.name)))
      
bandwidth = bandwidth_init()
create_tree(path,level=args.level)

def print_output(bandwidth):
    while True:
        time.sleep(args.timeout)
        bandwidth()
    
Thread(target=print_output, args=(bandwidth,), daemon=True).start()

queue.join()