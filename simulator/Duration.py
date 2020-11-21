from enum import Enum


class Duration(object):
    duration_id = 0

    class DurationType(Enum):
        Loss = 0
        Unavailable = 1
        SoftUpgrade = 2
        HardUpgrade = 3

    def __init__(self, d_type, start_time, end_time, unit, info=-100):
        self.type = d_type
        self.start_time = start_time
        self.end_time = end_time
        self.unit = unit
        self.info = info
        self.ignore = False
        self.attributes = {}
        Duration.duration_id += 1
        self.duration_id = Duration.duration_id

    def getType(self):
        return self.type

    def getStartTime(self):
        return self.start_time

    def getEndTime(self):
        return self.end_time

    def getUnit(self):
        return self.unit

    def getAttributes(self, key):
        if self.attributes == {}:
            return None
        return self.attributes[key]

    def setIgnoreToTrue(self):
        self.ignore = True

    def setAttributes(self, key, value):
        self.attributes[key] = value

    # start time + " " + end time + " " + unit + " " + type + " " + info + " "
    # + ignore
    def toString(self):
        format_string = str(self.start_time) + "  " + str(self.end_time) \
            + "  " + self.unit.toString() + "  " + str(self.type) + "  " \
            + str(self.info) + "  " + str(self.ignore) + "  " \
            + str(self.duration_id) + "\n"
        return format_string
