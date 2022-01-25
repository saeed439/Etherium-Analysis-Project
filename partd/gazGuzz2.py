from mrjob.job import MRJob
import re
from datetime import datetime
import time

class gazGuzz1(MRJob):

  def mapper(self, _, line):

    try:
        fields_view = line.split(',')

        if (len(fields_view) == 7) :
            time_epoch = int(fields_view[6])

            date = time.strftime("%b %Y", time.gmtime(time_epoch))

            gazP = float(fields_view[4])
                #year = time.strftime("%y",time.gmtime(timeu))
#getT = int(fields[6])

#month = timeu[20:24]


            yield (date, (gazP, 1))
    except:
        pass

  def combiner(self, date, values):

        countT = 0

        gazPT = 0

        for i in values:
            gazPT += i[0]
            countT += i[1]

        yield (date, (gazPT, countT))

  def reducer(self, date, values):

        countT = 0

        gazPT = 0

        for i in values:
            gazPT += i[0]
            countT += i[1]

        yield(date, (gazPT/countT))

if __name__ == '__main__':
    gazGuzz1.run()
