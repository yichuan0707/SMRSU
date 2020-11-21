from copy import deepcopy
from random import random, choice

from simulator.Configuration import Configuration
from simulator.Result import Result
from simulator.Duration import Duration
from simulator.DurationQueue import DurationQueue

from simulator.unit.Rack import Rack
from simulator.unit.Machine import Machine
from simulator.unit.Disk import Disk
from simulator.unit.Sector import Sector

from simulator.dataDistribute.SSSDistribute import SSSDistribute, HierSSSDistribute


class HandleDuration(object):

    def __init__(self, durations):
        self.durations = durations

        self.conf = Configuration()
        self.drs_handler = self.conf.getDRSHandler()
        self.isMDS = self.drs_handler.isMDS
        ft = self.drs_handler.n - self.drs_handler.k
        if self.isMDS:
            self.ft = ft
        else:
            # We only consider LRC with l = 2, so we
            # need to consider failures more than n-k-1
            self.ft = ft - 1

        self.concurrent_count = 0
        self.lost_concurrent_count = 0
        self.total_failure_slice_count = 0

    def returnConcurrentCount(self):
        return self.concurrent_count

    def returnFailureSliceCount(self):
        return self.total_failure_slice_count

    def isHandleLost(self):
        return self.handle_only_lost

    # Return concurrent durations
    # format: {(start time, end time):[list of units], ...}
    def findConcurrent(self):
        tmp_durations = []
        concurrent_durations = {}
        lost_concurrent_durations = {}
        last_concurrent_period = None

        durations = self.durations.clone()
        print "duration size:", durations.size()

        while durations.size() != 0:
            d = durations.removeFirst()
            current_time = d.getStartTime()

            for tmp_d in reversed(tmp_durations):
                if tmp_d.getEndTime() <= current_time:
                    tmp_durations.remove(tmp_d)

            tmp_durations.append(d)
            if len(tmp_durations) <= self.ft:
                continue

            concurrent_period = (max([tmp.getStartTime() for tmp in tmp_durations]),
                    min([tmp.getEndTime() for tmp in tmp_durations]))
            if last_concurrent_period is None:
                last_concurrent_period = concurrent_period
            else:
                if concurrent_period[0] < last_concurrent_period[1]:
                    pop_units = concurrent_durations.pop(last_concurrent_period)
                    concurrent_durations[(last_concurrent_period[0], concurrent_period[0])] = pop_units
                    if concurrent_period[1] < last_concurrent_period[1]:
                        concurrent_durations[(concurrent_period[1], last_concurrent_period[1])] = pop_units

            concurrent_units = [tmp.getUnit() for tmp in tmp_durations]
            concurrent_durations[concurrent_period] = concurrent_units

            if d.getType() == Duration.DurationType.Loss:
                lost_concurrent_durations[concurrent_period] = concurrent_units
                self.lost_concurrent_count += 1

            last_concurrent_period = concurrent_period
            self.concurrent_count += 1

        print "lost concurrent count:", self.lost_concurrent_count
        print "concurrent count:", self.concurrent_count
        return lost_concurrent_durations, concurrent_durations

    def process(self, concurrent_durations, distributer):
        total_failure_times = 0
        self.total_failure_slice_count = 0
        total_failure_period = 0.0
        # failure_period * failure_slice_count
        failure_period_with_weight = 0.0

        periods = concurrent_durations.keys()
        for period in periods:
            # key:slice_index, value:failure count in slice
            slice_failures = {}

            f_units = concurrent_durations[period]
            for u in f_units:
                if isinstance(u, Sector):
                    r = random()
                    # if no chunk is hited by sector error
                    if r > distributer.diskUsage():
                        continue
                    all_slices = u.parent.getChildren()
                    slice_index = choice(all_slices)
                    slice_failures.setdefault(slice_index, slice_failures.pop(slice_index, 0) + 1)
                else:
                    disks = []
                    if isinstance(u, Rack):
                        distributer.getAllDisksInRack(u, disks)
                    elif isinstance(u, Machine):
                        disks += u.getChildren()
                    elif isinstance(u, Disk):
                        disks.append(u)
                    else:
                        raise Exception("Invalid unit")

                    for disk in disks:
                        slices = disk.getChildren()
                        for slice_index in slices:
                            slice_failures.setdefault(slice_index, slice_failures.pop(slice_index, 0) + 1)

            failure_slice_count = 0
            slice_failure_in_period_flag = False
            failure_nums = slice_failures.values()
            for num in failure_nums:
                if self.isMDS and num > self.ft:
                    slice_failure_in_period_flag = True
                    failure_slice_count += 1
                if not self.isMDS and num > self.ft:
                    if num == self.ft + 1:
                        r2 = random()
                        if r2 < self.drs_handler.threshold:
                            slice_failure_in_period_flag = True
                            failure_slice_count += 1
                            print "random:%f, failures:%d,period:%f, threshold:%f" % (r2, failure_slice_count, period[1]-period[0], self.drs_handler.threshold)
                    else:
                        slice_failure_in_period_flag = True
                        failure_slice_count += 1
            self.total_failure_slice_count += failure_slice_count
            failure_period_with_weight += failure_slice_count*(period[1] - period[0])

            if slice_failure_in_period_flag:
                total_failure_times += 1
                total_failure_period += period[1] - period[0]

        return (total_failure_times, self.total_failure_slice_count, total_failure_period, failure_period_with_weight)

    # Get concurrent durations which contain $num$ failed units
    def getConcurrent(self, concurrent_durations, num):
        res = {}
        periods = concurrent_durations.keys()
        for period in periods:
            if len(concurrent_durations[period]) == num:
                res[period] = concurrent_durations[period]

        return res

    def printAll(self, concurrent_durations):
        i = 0
        periods = concurrent_durations.keys()
        periods.sort()
        for period in periods:
            format_string = str(period[0]) + "  " + str(period[1]) + " "
            for u in concurrent_durations[period]:
                format_string += " " + u.toString()
            print format_string

    def printToFile(self, file_path, concurrent_durations):
        pass
