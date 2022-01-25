from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class popularScams(MRJob):
    def steps(self):
        return [ #gets all mrstep function for this part
            MRStep(mapper=self.mapperScam,


                   reducer=self.reducerScam1),


            MRStep(mapper=self.mapperScam2,

            
                   reducer=self.reducerScam2)]
    def mapperScam(self, key, lines):
        try:


            fields_view = lines.split(",")


            if len(fields_view) == 7: # checks if its the right field

                val = float(fields_view[3]) #store val

                add = fields_view[2] # stores th address

                yield add, (val, 0)
            else:
                field_lines = json.loads(lines) #gets lines

                res = field_lines["result"]

                for i in res:


                    res1 = field_lines["result"][i]

                    category = res1["category"]

                    addresses = res1["addresses"]

                    for j in addresses:

                        yield j, (category, 1)
        except:


            pass
    def reducerScam1(self, key, values):

        cat1 = None

        val1 = 0

        for val2 in values:


            if val2[1] == 0:

                val1 = val1 + val2[0]

            else:

                cat1 = val2[0]

        if cat1 is not None:

            yield cat1, val1

    def mapperScam2(self, key, value):

        yield (key, value)


    def reducerScam2(self, key, value):

        yield (key, sum(value))


if __name__ == '__main__':
    popularScams.run()
