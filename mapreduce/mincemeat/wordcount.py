#!/usr/bin/env python
import mincemeat
from mincemeat_inputs import DictMapInput

data = ["Humpty Dumpty sat on a wall",
        "Humpty Dumpty had a great fall",
        "All the King's horses and all the King's men",
        "Couldn't put Humpty together again",
        ]
# The data source can be any dictionary-like object
datasource = dict(enumerate(data))

def mapfn(k, v):
    for w in v.split():
        yield w, 1

def reducefn(k, vs):
    result = sum(vs)
    return result

s = mincemeat.Server()
s.map_input = DictMapInput(datasource)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server()
print(results)