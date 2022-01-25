
from mrjob.job import MRJob

import re
from datetime import datetime
import time

WORD_REGEX = re.compile(r"\b\w+\b")
class partbB(MRJob):

    def mapper(self, _, line):
        try:
            if (len(line.split(','))  ==  5):

                fields_view=line.split(',')

                keyVal=fields_view[0]

                added_val=float(fields_view[3])


                yield (keyVal,(added_val,1))

            elif (len(line.split('\t')) == 2):

                fields_view=line.split('\t')

                keyVal=fields_view[0]

                keyVal=keyVal[1:-1] #join_key

                added_val=float(fields_view[1])

                yield (keyVal,(added_val,2))
        except:
            pass

    def reducer(self, address_Transfer, values):
        name = 0 #id
        allAdd = 0 #oi

        for i in values:

            if i[1]==1:
                name=i[0]

            elif i[1]==2:
               allAdd = i[0]

        if allAdd !=0 and name !=0:
            yield (to_address, allAdd)

if __name__=='__main__':
    partbB.run()
