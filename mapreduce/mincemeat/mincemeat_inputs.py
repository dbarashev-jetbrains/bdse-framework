import os
import codecs
from collections import deque

class MapInput:
    def next(self):
        pass

# This is a map input from in-memory dict structure. One map record is a dictionary entry.
class DictMapInput(MapInput):
    def __init__(self, datasource_dict):
        self.datasource_dict = datasource_dict
        self.dict_iter = iter(datasource_dict)

    def next(self):
        key = next(self.dict_iter)
        return key, self.datasource_dict[key]

# This is a map input from the files stored in a directory. One map record is the contents of a single file.
class FileMapInput(MapInput):
    currentFile = None
    lineCount = 0

    def __init__(self, directory):
        self.input_dir = directory
        self.filenames = deque([])
        for f in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, f)):
                self.filenames.append(os.path.join(directory, f))

    def next(self):
        if len(self.filenames) == 0:
            raise StopIteration
        filename = self.filenames.popleft()
        f = codecs.open(filename, "r", "utf-8")
        print(f'file {f.name}')
        return f.name,f.read()

# This is a map entry from the files stored in a directory.
# One map record is a single line from a file.
class FileMapInputLineByLine(MapInput):
    currentFile = None
    lineCount = 0

    def __init__(self, directory):
        self.input_dir = directory
        self.filenames = deque([])
        for f in os.listdir(directory):
            if not os.path.isdir(f):
                self.filenames.append(os.path.join(directory, f))

    def next(self):
        if self.currentFile is not None:
            nextLine = self.currentFile.readline()
            if nextLine != '':
                self.lineCount += 1
                #                if self.lineCount % 100 == 0:
                #                    print self.lineCount, ' lines'
                return self.currentFile.name, nextLine
            else:
                self.currentFile.close()

        if len(self.filenames) == 0:
            raise StopIteration
        filename = self.filenames.popleft()
        self.currentFile = codecs.open(filename, "r", "utf-8")
        print(f'next file={self.currentFile.name}')
        return next(self.currentFile)

# This is a map input that provides pairs (directory, filename) for each file in the given directory.
class FileNameMapInput(MapInput):
    def __init__(self, directory):
        self.input_dir = directory
        self.filenames = deque([])
        for f in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, f)):
                self.filenames.append(f)

    def has_next(self):
        return len(self.filenames) > 0

    def size(self):
        return len(self.filenames)

    def next(self):
        if len(self.filenames) == 0:
            raise StopIteration
        filename = self.filenames.popleft()
        return self.input_dir, filename
