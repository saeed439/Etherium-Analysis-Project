
from mrjob.job import MRJob
import re
from datetime import datetime
import time

WORD_REGEX = re.compile(r"\b\w+\b")

class partA(MRJob):

    def mapper(self, _, line):
        try:
            field_view = line.split(",")
            if len(field_view) == 7: #checks if the fields match the
                time_epoch = int(field_view[6]) #gets time
                timeGiven = time.ctime(time_epoch) #transforms time into readable form
                date = datetime.fromtimestamp(time_epoch).strftime("%m/%y") #returns date
                year = time.strftime("%y",ontracts (time.gmtime(time_epoch)) #returns year
                avg= float(field_view[3]) returns average #returns average of the transactions

                yield ((timeGiven[20:24], timeGiven[4:7]) , (avg, 1) ) #yields to the reducer and combiner
                #year = time.strftime("%y",time.gmtime(timeu))
#getT = int(fields[6])

#month = timeu[20:24]


        except:
            pass

    def combiner(self, key, values): #combiner function
        i = 0 #counter of the total times

        y = 0 #total of everything
        for value in values:

            i += value[1]

            y += value[0]

        yield (key,(y, i))

    def reducer(self, key, values): #reducer
        i = 0

        y = 0
        for value in values: #yields all values for avg

            i += value[1]

            y += value[0]

        yield (key, ((y/i), i))


if __name__ == '__main__':
    partA.run()
