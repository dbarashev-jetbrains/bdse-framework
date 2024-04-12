#!/usr/bin/env python 
import mincemeat
import mincemeat_inputs
import os
import sys


def mapfn(k, v):
    import great_bigrams
    import codecs

    f = codecs.open(os.path.join(k, v), "r", "utf-8")
    print(f'file {f.name}')
    counts = great_bigrams.do_map(f.read())
    for val,count in counts.items():
        yield val,count

def reducefn(k, vs):
    result = sum(vs)
    return result

s = mincemeat.Server() 

s.map_input = mincemeat_inputs.FileNameMapInput(sys.argv[1])
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme") 
mincemeat.dump_results(results)
