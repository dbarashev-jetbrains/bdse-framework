import logging

def count(val, counts):
    if val in counts:
        counts[val] += 1
    else:
        counts[val] = 1

def do_map(text):
    counts = {}
    do_map_with_counts(text, counts)
    return counts
        
def do_map_with_counts(text, counts):
    lines = text.splitlines()
    logging.debug('read %d lines' % len(lines))
    for line in lines:
        key, sep, tail = line.partition(' ');
        key = key.lower()
        if key.startswith('great'):
            val, _, _ = tail.partition('\t')
            if len(val) >= 2:                
                underscore = key.find('_')
                if underscore > 0:
                    key = key[:underscore]
                    
                if key == 'great':
                    count(val[0], counts)
                    count(val[:2], counts)


