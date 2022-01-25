from mrjob.job import MRJob
import re
from datetime import datetime
import time

WORD_REGEX = re.compile(r"\b\w+\b")

class TopMiners(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split('\t')

            if len(fields) == 2:

                add = fields[0]

                val = float(fields[1])

                yield (None, (add, val))

        except :

            pass

    def combiner(self, _, values):

        valS = sorted(values, reverse = True, key = lambda tup:tup[1])

        i = 0

        for val in valS:
            yield ("top", val)
            i += 1
            if i >= 10:
                break

    def reducer(self, _, values):

        valS = sorted(values, reverse = True, key = lambda tup:tup[1])

        i = 0
        
        for val in valS:
            yield i, ("{}{}".format(val[0],val[1]))
            i += 1
            if i >= 10:
                break


if __name__ == '__main__':
    TopMiners.run()
