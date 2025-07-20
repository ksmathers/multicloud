

class TableNames:
    """
    TableNames implements a symbol table with dependency tree.   Use the 'add' method to
    add a table and its dependencies to the tree one dependency at a time.   The source code 
    filename where the table is defined can be provided as well to help locate where a table 
    definition can be changed.
    """
    def __init__(self):
        self.tables = {}
        self.files = {}
        
    def add(self, fname, table, dep):
        if table not in self.tables:
            self.tables[table] = []
            self.files[table] = fname
        if dep not in self.tables[table]:
            self.tables[table].append(dep)
            
    def names(self):
        return list(self.tables.keys())

    def source(self, tbl):
        if tbl not in self.files:
            return None
        return self.files[tbl]
    
    def dependencies(self, tbl: str, depth=0):
        deps = []
        leaf = False
        if tbl not in self.tables:
            if tbl.endswith("_prepared"):
                # prepared outputs are sometimes automatically promoted to processed.  Why?
                tbl = tbl.replace("_prepared", "_processed")
            if tbl not in self.tables:
                leaf = True
                #print("  "*indent, "**Unable to find",tbl)

        deps.append({
            'leaf': leaf, 
            'depth': depth, 
            'node': tbl
            })
        
        if not leaf:
            for x in self.tables[tbl]:
                deps.extend(self.dependencies(x, depth+1))
        return deps

    def substitute(self, node, substitutions, reverse=False):
        if reverse:
            for sub in substitutions[::-1]:
                node = node.replace(sub[1], sub[0])   
        else:
            for sub in substitutions:
                node = node.replace(sub[0], sub[1])      
        return node

    def show_dependencies(self, tbl: str, mark_repeat="Ref:", mark_leaf="- ", mark_nonleaf="+ ", mark_depth="  ", skip_repeats=True, substitutions=[]):
        tbl = self.substitute(tbl, substitutions, reverse=True)
        deps = self.dependencies(tbl)
        shown = []
        undepth = 999
        for d in deps:
            depth = d['depth']
            leaf = d['leaf']
            node = str(d['node'])
            isshown = node in shown
            
            if (depth > undepth and skip_repeats):
                continue

            undepth=999
            if depth > 0:
                print(mark_depth*depth, end="")

            if leaf:
                print(mark_leaf, end="")
            else:
                print(mark_nonleaf, end="")

            if isshown:
                undepth=depth
                print(mark_repeat, end="")

            shown.append(node)
            for sub in substitutions:
                node = node.replace(sub[0], sub[1])
            print(node)
            
            

        
