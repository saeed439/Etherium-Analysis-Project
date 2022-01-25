from mrjob.job import MRJob
import re
from datetime import datetime
import time

WORD_REGEX = re.compile(r"\b\w+\b")

class partc(MRJob):

    def mapper(self, _, line):
        try:
            field_view = line.split(",")
            if (len(field_view) == 9):

                miner = field_view[2]


                size = int(field_view[4])



                yield (miner, size)
                #year = time.strftime("%y",time.gmtime(timeu))
#getT = int(fields[6])

#month = timeu[20:24]
        except:
            pass

    def combiner(self, word, counts):
        total = sum(counts)

        
        yield (word, total)


    def reducer(self, word, counts):
        total = sum(counts)
        yield (word, total)


if __name__ == '__main__':
    partc.run()
