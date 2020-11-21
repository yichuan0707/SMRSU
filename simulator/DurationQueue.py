from collections import OrderedDict
from simulator.Duration import Duration


class DurationQueue(object):
    durations = OrderedDict()

    def addDuration(self, d):
        if d.getStartTime() in DurationQueue.durations.keys():
            DurationQueue.durations.get(d.getStartTime()).append(d)
        else:
            duration_list = []
            duration_list.append(d)
            DurationQueue.durations.setdefault(d.getStartTime(), duration_list)

    def addDurationQueue(self, queue):
        all_durations = queue.getAllDurations()
        for d in all_durations:
            self.addDuration(d)

    def remove(self, d):
        ts = d.getStartTime()
        durations = DurationQueue.durations.get(ts)
        durations.remove(d)
        if len(durations) == 0:
            DurationQueue.durations.pop(ts)

    def removeFirst(self):
        if DurationQueue.durations.keys() == []:
            return None

        # pop and deal with duration based on the timestamp, so we need to
        # sort at first.
        keys = DurationQueue.durations.keys()
        keys.sort()
        first_key = keys[0]

        first_value = DurationQueue.durations[first_key]
        first_duration = first_value.pop(0)
        if len(first_value) == 0:
            DurationQueue.durations.pop(first_key)

        return first_duration

    def getAllDurations(self):
        res = []
        for d in DurationQueue.durations.values():
            res.append(d)
        return res

    def convertToArray(self):
        duration_list = []
        # check if we can operate OrderedDict like this?
        # Yes, we can. This is normal operation for Dict.
        iterator = DurationQueue.durations.itervalues()
        for l in iterator:
            for d in l:
                duration_list.append(d)

        return duration_list

    # override?
    def clone(self):
        ret = DurationQueue()
        keys = DurationQueue.durations.keys()
        keys.sort()
        # this place will be self.durations or DurationQueue.durations?
        for ts in keys:
            list1 = DurationQueue.durations.get(ts)
            list2 = []
            for d in list1:
                list2.append(d)
            ret.durations.setdefault(ts, list2)

        return ret

    def size(self):
        size = 0
        for item in DurationQueue.durations.values():
            size += len(item)
        return size

    def printAll(self, file_name, msg):
        with open(file_name, 'w+') as out:
            out.write(msg + "\n")
            keys = DurationQueue.durations.keys()
            keys.sort()
            for t in keys:
                res = DurationQueue.durations[t]
                for d in res:
                    if d.ignore is False:
                        out.write(d.toString())

    def printDurations(self, file_name, msg, duration_type=Duration.DurationType.Unavailable, sort=True):
        with open(file_name, 'w') as fp:
            fp.write(msg + "\n")
            keys = DurationQueue.durations.keys()
            if sort:
                keys.sort()
            for t in keys:
                res = DurationQueue.durations[t]
                for d in res:
                    if (d.ignore is False) and \
                            d.getType() == duration_type:
                        fp.write(d.toString())

