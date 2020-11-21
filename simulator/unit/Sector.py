from simulator.unit.Unit import Unit


class Sector(Unit):

    def __init__(self, name, parent):
        super(Sector, self).__init__(name, parent, None)
        self.original_failure_time = 0

    def getOriginalFailureTime(self):
        return self.original_failure_time

    def setOriginalFailureTime(self, original_failure_time):
        self.original_failure_time = original_failure_time

