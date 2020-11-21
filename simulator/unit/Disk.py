from simulator.unit.Unit import Unit
from simulator.Duration import Duration
from simulator.Configuration import Configuration


class Disk(Unit):

    def __init__(self, name, parent, parameters):
        super(Disk, self).__init__(name, parent, parameters)
        conf = Configuration()
        self.disk_capacity = conf.disk_capacity
        self.disk_repair_time = conf.disk_repair_time
        self.slices_hit_by_LSE = []

    def setDiskCapacity(self, disk_capacity):
        self.disk_capacity = disk_capacity

    def getDiskCapacity(self):
        return self.disk_capacity

    def generateDurations(self, result_durations, start_time, end_time, reset):
        if start_time < self.start_time:
            start_time = self.start_time
        current_time = start_time
        last_recover_time = start_time

        while True:
            self.failure_generator.reset(current_time)
            failure_time = self.failure_generator.generateNextEvent(
                current_time)
            current_time = failure_time
            if current_time > end_time:
                break

            self.recovery_generator.reset(current_time)
            recovery_time = self.recovery_generator.generateNextEvent(
                current_time)
            assert (recovery_time > failure_time)
            # only failure identification time has been given by recovery generator, we add data transfer time here.
            recovery_time += self.disk_repair_time

            current_time = failure_time
            disk_duration = Duration(Duration.DurationType.Loss, current_time, recovery_time, self)
            result_durations.addDuration(disk_duration)

            current_time = recovery_time
            if current_time > end_time:
                break
            last_recover_time = current_time
