import os
import pathlib
from generic_templates.text_finder import TextFinder
from .tablenames import TableNames


class FoundryTransforms(TableNames):
    def __init__(self, foundry_repos = [], verbose = False):
        super().__init__()
        if len(foundry_repos)==0:
            foundry_repos.extend([
                "~/git/foundry/workflow_oa_repository",
                "~/git/foundry/datasource_pronto_repository",
                "~/git/foundry/transform_pronto_repository",
                "~/git/foundry/sap_repository",
                "~/git/foundry/transform_electric_transmission_repository",
                "~/git/foundry/ontology_electric_transmission_repository",
            ])
        
        self.verbose = verbose
        
        # map the path strings and validate
        paths = [ os.path.expanduser(x) for x in foundry_repos ]
        for path in paths:
            assert(os.path.isdir(path))

        for path in paths:
            for fname, sql in self.load_files(path):
                self._intern(fname, sql)

    def find_files(self, base):
        for fname in list(pathlib.Path(base).glob("**/*.py")):
            yield fname
            
    def load_files(self, base):
        for fname in self.find_files(base):
            text = open(fname, 'r').read()
            yield fname, text

    def find_tables(self, sql:str):
        tlist = []
        tf = TextFinder(sql)
        try:
            tf.find("@transform_df")
        except ValueError:
            # not a recognized transform
            return []
                
        try:
            output = tf.dequotex(r"""Output\( *['"]""", r"""['"]\)""").copy()
            tlist.append(output)
        except:
            print("no output: ", tf.mark().skip(100).copy())
            raise Exception()
        
        while True:
            try:
                tbl = tf.dequotex(r"""Input\( *['"]""", r"""['"]\)""").copy()
                if tbl not in tlist:
                    tlist.append(tbl) 
            except ValueError:
                break
        return tlist

    def _intern(self, fname, text):
        tlist = self.find_tables(text)
        if len(tlist)==0: return
        if self.verbose:
            print(fname)
        mytable = tlist.pop(0)
        others = sorted(list(set(tlist) - set(mytable)))
        for o in others:
            self.add(fname, mytable, o)

