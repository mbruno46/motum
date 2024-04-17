import subprocess
import os
import time
import sys

from queue import Queue
from threading import Thread

def background_process(func, args=()):
    Thread(target=func, args=args, daemon=True).start()

class Log:
    def __init__(self, verbose):
        self.verbose = verbose
        
    def __call__(self, msg, default=True):
        if (default) or (self.verbose):
            print(f'[omtum]: {msg}')
        
    def same_line(self, msg):
        sys.stdout.write(f'\r[motum]: {msg}')
        sys.stdout.flush()
        
log = Log(False)

class MoveTask:
    def __init__(self, arg, host, dst):
        self.arg = arg
        self.host = host
        self.dst = dst
        
    def __call__(self):
        cmd = ['rsync','-acz','-pgot',self.arg,'-e','ssh',f'{self.host}:{self.dst}']
        print(f'[motum]: starting transfer of {self.arg}')
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
        
class Job:
    def __init__(self, n, path, host, dst):
        self.n = n
        self.path = path
        self.host = host
        self.dst = dst
        self.queue = Queue()
        for _ in range(self.n):
            background_process(worker, (self.queue,))

        log(f'{path} -> {host}:{dst}')

    def populate(self, path, level):    
        with os.scandir(path) as it:
            for entry in it:
                if entry.name.startswith('.'):
                    continue

                if level>1:
                    self.populate(os.path.join(path,entry.name), level-1)
                else:
                    self.queue.put(MoveTask(os.path.join(path,entry.name), self.host, self.dst))

    def size(self):
        return int(os.popen(f'du -sk "{self.path}"').read().split()[0])
    
    def remote_size(self):
        return int(os.popen(f'ssh {self.host} "du -sk {self.dst}" 2> /dev/null').read().split()[0])
    
    def __call__(self, level):
        self.populate(self.path,level)
        self.queue.join()

def Monitor(job, verbose, timeout):
    log.verbose = verbose
    t0 = time.time()
    
    full = job.size()
    log(f'total size {full/1024:.2f} MB')

    def inner():
        tot = job.remote_size()
        log(f'transferred size {tot/1024:.2f} MB', False)
        dt = time.time() - t0
        log.same_line(f'effective bandwidth {tot/dt/1024:.2f} MB/sec ; {int(tot/full*100): <3d} % completed')
        
    def runner():
        while True:
            time.sleep(timeout)
            inner()
            
    background_process(runner)
