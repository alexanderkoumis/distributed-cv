
import os
import re
from mrjob.job import MRJob

WORD_RE = re.compile(r'[\w]+')

class MRWordCountLookup(MRJob):
    """Takes a list of files as input to then count the words from.
    """

    def mapper_init(self):
        self.words = {}

    def mapper(self, _, line):
        path = line.strip()
        if not os.path.isfile(path):
            return
        with open(path, 'r') as file_d:
            data = file_d.read()
        for word in WORD_RE.findall(data):
            word = word.lower()
            self.words.setdefault(word, 0)
            self.words[word] = self.words[word] + 1

    def mapper_final(self):
        for word, val in self.words.iteritems():
            yield word, val

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == '__main__':
    MRWordCountLookup.run()


