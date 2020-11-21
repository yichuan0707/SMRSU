from copy import deepcopy

from simulator.unit.Unit import Unit
from simulator.Duration import Duration


class Rack(Unit):

    def __init__(self, name, parent, parameters):
        super(Rack, self).__init__(name, parent, parameters)
        # If True, rack failure durations will be generated
        # but ignored(neither handled nor written to file).
        self.fast_forward = bool(parameters.get("fast_forward"))

    def generateDurations(self, result_durations, start_time, end_time, reset):
        if start_time < self.start_time:
            start_time = self.start_time
        current_time = start_time
        last_recover_time = start_time

        if self.failure_generator is None:
            for u in self.children:
                u.generateDurations(result_durations, start_time, end_time, True)
            return

        while True:
            if reset:
                self.failure_generator.reset(current_time)
            failure_time = self.failure_generator.generateNextEvent(
                current_time)
            current_time = failure_time
            self.recovery_generator.reset(current_time)
            recovery_time = self.recovery_generator.generateNextEvent(
                current_time)
            assert (recovery_time > failure_time)
            if current_time > end_time:
                for u in self.children:
                    u.generateDurations(result_durations, last_recover_time,
                                     end_time, True)
                break

            # We only consider transient failures of racks
            rack_duration = Duration(Duration.DurationType.Unavailable, failure_time, recovery_time, self)
            result_durations.addDuration(rack_duration)
            if self.fast_forward:
                rack_duration.ignore = True

            for u in self.children:
                u.generateDurations(result_durations, last_recover_time,
                                 failure_time, True)

            current_time = recovery_time


            if current_time > end_time:
                break
            last_recover_time = current_time

    def toString(self):
        full_name = super(Rack, self).toString()
        parts = full_name.split(".")
        return ".".join(parts[2:])
