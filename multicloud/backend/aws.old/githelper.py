import subprocess
import os


class GitHelper:
    def __init__(self, basedir):
        self.basedir = basedir
        
    def _pipe(self, cmd, **kwargs):
        args = cmd.split(" ")
        xargs = []
        for i,arg in enumerate(args):
            if arg.startswith("@"):
                kwarg = kwargs.get(arg[1:])
                if kwarg is None:
                    raise Exception(f"Substitution argument ''{arg}'' is missing from call parameters")
                xargs.append(kwarg)
            elif arg == "git":
                xargs.append("git")
                xargs.append(f"--git-dir={self.basedir}/.git/")
            else:
                xargs.append(arg)
                
        h = subprocess.run(xargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = h.stdout.decode('utf-8').split("\n")
        stderr = h.stderr.decode('utf-8').split("\n")
        return output + stderr + ["Success" if h.returncode == 0 else "Failed"]
        
        
    def log(self):
        output = self._pipe("git log")
        return output
    
    def last_commit(self):
        log = self.log()
        for ll in log:
            if (ll.startswith('commit')):
                return ll.split(" ")[1]
        return None
    
    def diff(self, revision_hash):
        output = self._pipe("git diff -r @rev1 -r @rev2", rev1=revision_hash+"^", rev2=revision_hash)
        return output
    
    def commit_files(self, revision_hash):
        filelist = []
        diff = self.diff(revision_hash)
        after = 0
        for ll in diff:
            if ll.startswith('diff'):
                fname = "./" + ll.split(" b/")[1]
                after=1
            elif after>0:
                filelist.append({'fname': fname, 'op': ll})
                after-=1
        return filelist
                
    def last_commit_files(self):
        return self.commit_files(self.last_commit())

    def clone_repository(self, githandle, gitdir):
        if gitdir.startswith("/"):
            githome=gitdir
        else:
            githome = os.path.join(os.environ["HOME"], "git-helper", gitdir)

        os.makedirs(githome, exist_ok=True)
        #os.chdir(githome)
        output = self._pipe("git clone @githandle @githome", githandle=githandle, githome=githome)
        return output
