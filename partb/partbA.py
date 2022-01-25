from mrjob.job import MRJob
import re
from datetime import datetime
import time

WORD_REGEX = re.compile(r"\b\w+\b")



class partbA(MRJob):

    def mapper(self, _, line):


        try:


            fields = line.split(',')
            if len(fields) == 7:
                value_Transfer = float(fields[3])
                if value_Transfer == 0:

                    pass
                address_Transfer = fields[2]

                else:
                    yield (address_Transfer, value_Transfer)
        except:


            pass

    def combiner (self, address_Transfer, value_Transfer):
            final = sum(value_Tranfser)



            yield (address_Transfer, final)

    def reducer (self, address_Transfer, value_Transfer):
            final = sum(value_Tranfser)



            yield (address_Transfer, final)

if __name__ == '__main__':
    partbA.run()
