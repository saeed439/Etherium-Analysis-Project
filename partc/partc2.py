from mrjob.job import MRJob
import re
from datetime import datetime
import time

WORD_REGEX = re.compile(r"\b\w+\b")

class partc2(MRJob):

    def mapper(self, _, line):
        try:

            fields = line.split('\t')

            if len(fields) == 2:


                miner = fields[0]

                size = float(fields[1])

                yield (None, (miner, size))
        except :


            pass

    def combiner(self, _, values):


        valS = sorted(values, reverse = True, key = lambda tup:tup[1])


        i = 0

        for value in valS:

            yield ("top", value)

            i += 1

            if i >= 10:

                break

    def reducer(self, _, values):

        valS = sorted(values, reverse = True, key = lambda tup:tup[1])


        i = 0

        for value in valS:

            yield i, (" {}  {}".format(value[0],value[1]))

            i += 1

            if i >= 10:

                
                break


if __name__ == '__main__':
    partc2.run()
