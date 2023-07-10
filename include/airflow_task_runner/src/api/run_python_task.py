import sys
import json
import subprocess

if __name__ == "__main__":

    cmd = json.loads(sys.argv[1])

    proc: subprocess.Popen = subprocess.Popen(cmd)