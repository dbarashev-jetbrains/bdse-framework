import codecs
import optparse
import os
from collections import deque

import great_bigrams

class Counter:
    def __init__(self, directory):
        self.input_dir = directory
        self.filenames = deque([])
        self.counts = {}
        
        for f in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, f)):
                self.filenames.append(os.path.join(directory, f))

    def next(self):
        if len(self.filenames) == 0:
            raise StopIteration
        filename = self.filenames.popleft() 
        f = codecs.open(filename, "r", "utf-8")
        print 'file ', f.name
        return f.name,f.read()
        
    def mapfn(self, k, v):
        great_bigrams.do_map_with_counts(v, self.counts)
                    
    def dump_results(self):
        for val,count in sorted(self.counts.items()):
            print val, '\t', count

    def process(self):
        while len(self.filenames) > 0:
            k, v = self.next()
            self.mapfn(k, v)
        self.dump_results()
        
if __name__ == '__main__':
    parser = optparse.OptionParser()
    (options, args) = parser.parse_args()
    counter = Counter(args[0])
    counter.process()    

