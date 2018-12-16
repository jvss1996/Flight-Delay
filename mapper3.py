#!/usr/bin/env python
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp

class Mapper(api.Mapper):
        def map(self, context):
                words = context.value.split(' ')
                for w in words:
                        line = w.split(',')
                        if (line[15] >= '1'):
                                #delay = 1, not_delay = 0
                                context.emit(line[16], 11)
                        else:
                                context.emit(line[16], 10)

class Reducer(api.Reducer):
        def reduce(self, context):
                s = 0.0
                quotient = 0.0
                remainder = 0.0
                for i in context.values:
                        remainder += int(i%10)
                        quotient += int(i/10)
                s = ((remainder*100)/quotient)
                context.emit(context.key, s)

def __main__():
        pp.run_task(pp.Factory(Mapper, Reducer))