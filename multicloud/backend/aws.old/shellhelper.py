import subprocess
import os
import shlex
from typing import List

class ShellResult:
    def __init__(self, cmd : List[str], handle : subprocess.CompletedProcess):
        self.cmd = cmd
        if handle.stdout:
            self.output = handle.stdout.decode('utf-8').split("\n")
        else:
            self.output = []
        if handle.stderr:
            self.stderr = handle.stderr.decode('utf-8').split("\n")
        else:
            self.stderr = []
        self.exitcode = handle.returncode

    @property
    def success(self):
        return self.exitcode == 0
    
    @property
    def log(self):
        return self.output + self.stderr + ["Success" if self.success else "Failed"]
    
    def print_log(self, line_prefix : str = ":", vgrep : str = None, egrep : str = None):
        for ll in self.log:
            if egrep and not egrep in ll: continue
            if vgrep and vgrep in ll: continue
            print(line_prefix+ll)

class ShellHelper:
    def __init__(self, chdir=".", shell=False):
        self.basedir = chdir
        self.shell = shell
        
    def sh(self, cmd, **kwargs):
        args = shlex.split(cmd)
        xargs = []
        for i,arg in enumerate(args):
            if arg.startswith("@"):
                kwarg = kwargs.get(arg[1:])
                if kwarg is None:
                    raise Exception(f"Substitution argument ''{arg}'' is missing from call parameters")
                xargs.append(kwarg)
            else:
                xargs.append(arg)
                
        h = subprocess.run(xargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=self.basedir, shell=self.shell)
        return ShellResult(xargs, h)

