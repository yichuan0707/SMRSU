from random import random
from decimal import *

from simulator.failure.EventGenerator import EventGenerator


class WeibullGenerator(EventGenerator):
    """
    Weibull Distribution.
    """
    def __init__(self, name, parameters):
        self.name = name
        self.gamma = Decimal(parameters['gamma'])
        self.lamda = Decimal(parameters['lamda'])
        self.beta = Decimal(parameters['beta'])
        self.start_time = Decimal(0)

    def getName(self):
        return self.name

    def getCurrentTime(self):
        return self.start_time

    def reset(self, current_time):
        self.start_time = Decimal(current_time)

    def getRate(self):
        return self.lamda

    def F(self, current_time):
        t = -pow(current_time/self.lamda, self.beta)
        return Decimal(1.0) - t.exp()

    def generateNextEvent(self, current_time):
        c_time = Decimal(current_time) - self.start_time
        if c_time < 0:
            raise Exception("Negative current time!")

        r = random()
        R = (Decimal(1) - self.F(c_time)) * Decimal(r) + self.F(c_time)
        tmp = pow(-Decimal(1-R).ln(), Decimal(1)/self.beta)
        result = self.lamda*tmp + self.gamma + self.start_time

        if result < 0:
            raise Exception("Generated time is negative")
        return round(result, 2)


def main():
    w = WeibullGenerator("wei", {'gamma': 0.02, 'lamda': 0.03, 'beta': 1})
    hist = {}
    for i in xrange(1000):
        next_event = w.generateNextEvent(0.0)
        next_event = float(next_event*10000.0)/10000.0
        if next_event in hist.keys():
            p = hist.get(next_event)
            hist[next_event] = p + 1.0
        else:
            hist[next_event] = 1.0

    for i in hist.items():
        print str(i[0]) + "  " + str(i[1])

def test():
    w = WeibullGenerator("wei", {'gamma': 0.0, 'lamda': 9259, 'beta': 1})
    current_time = 0.0
    TTF = []

    while current_time <= 87600.0:
        next_event = w.generateNextEvent(current_time)
        next_event = float(next_event*10000.0)/10000.0
        TTF.append(next_event - current_time)
        current_time = next_event

    TTF.sort()
    MTTF = sum(TTF)/len(TTF)
    print TTF
    print "MTTF:", MTTF
    return MTTF


if __name__ == "__main__":
    w = WeibullGenerator("wei", {'gamma': 6.0, 'lamda': 336, 'beta': 3.0})
    current_time = 1102.99
    print "F return:", w.F(current_time)
    print "next:", w.generateNextEvent(current_time)

