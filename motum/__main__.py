import time
import os

from .motum import Job, Monitor
from argparse import ArgumentParser

############

parser = ArgumentParser(prog='motum',description='Parallel data mover')
parser.add_argument("src",help="path of the source folder")
parser.add_argument("host",help="ssh destination server (username)@hostname")
parser.add_argument("dst",help="path of the destination folder")

parser.add_argument("-v","--verbose",action='store_true')

parser.add_argument("-n",default=1,type=int,help="number of parallel streams; default=1")
parser.add_argument("--timeout",default=5,type=int,help="time interval in seconds of stdout messages; default=5")
parser.add_argument("--level",default=1,type=int,help='depth of folder recursion; default=1')

args = parser.parse_args()

path = os.path.abspath(args.src)
host = args.host

_dst = os.popen(f'ssh {host} "echo {args.dst}" 2> /dev/null').read().strip()
dst = os.path.join(_dst, os.path.basename(path) + '/').replace(' ','\ ')
   
job = Job(args.n, path, host, dst)

Monitor(job, args.verbose, args.timeout)

job(args.level)