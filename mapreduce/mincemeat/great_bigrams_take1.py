#!/usr/bin/env python 
import mincemeat
import mincemeat_inputs
import sys

def mapfn(k, v):
    import great_bigrams
    counts = great_bigrams.do_map(v)
    for key, value in counts.items():
        yield key, value

def reducefn(k, vs):
    result = sum(vs)
    return result

s = mincemeat.Server() 

s.map_input = mincemeat_inputs.FileMapInputLineByLine(sys.argv[1])
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme") 
for key, value in sorted(results.items()):
    print(f"{key}: {value}")
