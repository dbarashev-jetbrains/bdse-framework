#!/usr/bin/env python 
import mincemeat
import mincemeat_inputs
import sys

def mapfn(k, v):
    import great_bigrams
    counts = great_bigrams.do_map(v)
    for val,count in counts.items():
        yield val,count

def reducefn(k, vs):
    result = sum(vs)
    return result

s = mincemeat.Server() 

s.map_input = mincemeat_inputs.FileMapInput(sys.argv[1])
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server()
mincemeat.dump_results(results)
