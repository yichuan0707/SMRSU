from random import uniform

from simulator.failure.EventGenerator import EventGenerator


class Real(EventGenerator):
    """
    Random failure detection time
    """

    def __init__(self, name, parameters):
        self.name = name
        # maximum detection time
        self.gamma = float(parameters['gamma'])
        # identification time
        self.lamda = float(parameters['lamda'])

    def reset(self, current_time):
        pass

    def getName(self):
        return self.name

    def getCurrentTime(self):
        return 0

    def generateNextEvent(self, current_time):
        return round(current_time + uniform(0, self.gamma) + self.lamda, 2)
