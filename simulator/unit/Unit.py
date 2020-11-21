from abc import ABCMeta
from copy import deepcopy

from simulator.Duration import Duration


class Unit:
    __metaclass__ = ABCMeta
    unit_count = 0

    def __init__(self, name, parent, parameters):
        self.children = []
        self.parent = parent
        self.name = name
        self.failure_generator = None
        self.recovery_generator = None

        self.id = Unit.unit_count
        self.start_time = 0
        self.end_time = None
        self.last_failure_time = 0
        self.last_bandwidth_need = 0

        Unit.unit_count += 1

    def __eq__(self, other):
        return self.id == other.id

    def setStartTime(self, ts):
        self.start_time = ts
        if self.children != []:
            for child in self.children:
                child.setStartTime(ts)

    def getStartTime(self):
        return self.start_time

    def setLastFailureTime(self, ts):
        self.last_failure_time = ts

    def getLastFailureTime(self):
        return self.last_failure_time

    def setLastBandwidthNeed(self, bw):
        self.last_bandwidth_need = bw

    def getLastBandwidthNeed(self):
        return self.last_bandwidth_need

    # unit must be a instance? Really?
    def addChild(self, unit):
        self.children.append(unit)

    def removeChild(self, unit):
        self.children.remove(unit)

    # return the amount of children for replacement
    def removeAllChildren(self):
        count = len(self.children)
        self.children = []
        return count

    def getChildren(self):
        return self.children

    def getParent(self):
        return self.parent

    def getID(self):
        return self.id

    def addEventGenerator(self, generator):
        if generator.getName() == "failureGenerator":
            self.failure_generator = generator
        elif generator.getName() == "recoveryGenerator":
            self.recovery_generator = generator
        else:
            raise Exception("Unknown generator" + generator.getName())

    def getEventGenerators(self):
        return [self.failure_generator, self.recovery_generator]

    def generateDurations(self, result_durations, start_time, end_time, reset):
        current_time = start_time
        last_recover_time = start_time

        if self.failure_generator is None:
            for unit in self.children:
                unit.generateDurations(result_durations, start_time, end_time, reset)
            return

        while True:
            if reset:
                self.failure_generator.reset(current_time)

            failure_time = self.failure_generator.generateNextEvent(
                current_time)
            current_time = failure_time
            if current_time > end_time:
                for u in self.children:
                    u.generateDurations(result_durations, last_recover_time,
                                     end_time, True)
                break
            for u in self.children:
                u.generateDurations(result_durations, last_recover_time,
                                 current_time, True)

            self.recovery_generator.reset(current_time)
            recovery_time = self.recovery_generator.generateNextEvent(
                current_time)
            assert (recovery_time > failure_time)
            current_time = recovery_time

            duration = Duration(Duration.DurationType.Unavailable, failure_time, recovery_time, self)
            if current_time > end_time:
                break

            last_recover_time = current_time

    def toString(self):
        if self.parent is None:
            return self.name
        else:
            return self.parent.toString() + '.' + self.name

    def printAll(self, prefix="--"):
        print prefix + self.name
        prefix += "--"
        for unit in self.children:
            if isinstance(unit, int):
                break
            unit.printAll(prefix)

