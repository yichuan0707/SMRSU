from random import random, uniform
from math import ceil
from copy import deepcopy

from simulator.unit.Unit import Unit
from simulator.Duration import Duration
from simulator.failure.Trace import Trace
from simulator.Configuration import Configuration


class Machine(Unit):
    id_counter = 0
    fail_fraction = 0.0

    def __init__(self, name, parent, parameters):
        self.my_id = Machine.id_counter
        Machine.id_counter += 1
        super(Machine, self).__init__(name, parent, parameters)

        # recovery generator for permanent machine failure
        self.recovery_generator2 = None

        # amount of time after which a machine failure is treated as permanent,
        # and eager disk recovery is begun, if eager_recovery_enabled is True.
        self.fail_timeout = -1
        if self.fail_timeout == -1:
            # Fraction of machine failures that are permanent.
            Machine.fail_fraction = float(parameters.get("fail_fraction", 0.008))
            self.fail_timeout = float(parameters.get("fail_timeout", 0.25))
            # If True, machine failure and recovery durations will be generated
            # but ignored.
            self.fast_forward = bool(parameters.get("fast_forward"))
            self.eager_recovery_enabled = bool(parameters.get(
                "eager_recovery_enabled"))

        self.fail_durations = []

        conf = Configuration()
        self.machine_repair_time = conf.node_repair_time

    def getFailureGenerator(self):
        return self.failure_generator

    def addEventGenerator(self, generator):
        if generator.getName() == "recoveryGenerator2":
            self.recovery_generator2 = generator
        else:
            super(Machine, self).addEventGenerator(generator)

    def getEventGenerators(self):
        return [self.failure_generator, self.recovery_generator, self.recovery_generator2]

    def generateDurations(self, result_durations, start_time, end_time, reset):
        if start_time < self.start_time:
            start_time = self.start_time
        current_time = start_time
        last_recover_time = start_time

        if self.failure_generator is None:
            for u in self.children:
                u.generateDurations(result_durations, start_time, end_time, True)
            return

        if isinstance(self.failure_generator, Trace):
            self.failure_generator.setCurrentMachine(self.my_id)
        if isinstance(self.recovery_generator, Trace):
            self.recovery_generator.setCurrentMachine(self.my_id)

        while True:
            if reset:
                self.failure_generator.reset(current_time)

            if isinstance(self.failure_generator, Trace):
                # For the event start.
                self.failure_generator.setCurrentEventType(True)

            failure_time = self.failure_generator.generateNextEvent(
                current_time)
            current_time = failure_time
            if current_time > end_time:
                for u in self.children:
                    u.generateDurations(result_durations, last_recover_time,
                                     end_time, True)
                break

            if isinstance(self.failure_generator, Trace):
                self.failure_generator.eventAccepted()

            if isinstance(self.recovery_generator, Trace):
                self.recovery_generator.setCurrentEventType(False)
            self.recovery_generator.reset(current_time)
            recovery_time = self.recovery_generator.generateNextEvent(
                current_time)
            assert (recovery_time > failure_time)

            for u in self.children:
                u.generateDurations(result_durations, last_recover_time,
                                 failure_time, True)

            if recovery_time > end_time:
                recovery_time = end_time

            r = random()
            if not self.fast_forward:  # we will process failures
                if r < Machine.fail_fraction:
                    # failure type: tempAndShort=1, tempAndLong=2, permanent=3
                    failure_type = 3

                    # detection time and identification time comes from recovery_generator2
                    recovery_time = self.recovery_generator2.generateNextEvent(
                            failure_time) + self.machine_repair_time
                    machine_duration = Duration(Duration.DurationType.Loss,
                            failure_time, recovery_time, self, failure_type)
                else:
                    if recovery_time - failure_time <= self.fail_timeout:
                        # transient failure and come back very soon
                        failure_type = 1
                        machine_duration = Duration(Duration.DurationType.Unavailable,
                                failure_time, recovery_time, self, failure_type)
                    else:
                        # transient failure, but last long.
                        failure_type = 2
                        if self.eager_recovery_enabled:
                            recovery_time = failure_time + self.fail_timeout + self.machine_repair_time
                            machine_duration = Duration(Duration.DurationType.Loss,
                                    failure_time, recovery_time, self, failure_type)
                        else:
                            machine_duration = Duration(Duration.DurationType.Unavailable,
                                    failure_time, recovery_time, self, failure_type)

            if isinstance(self.failure_generator, Trace):
                self.failure_generator.eventAccepted()

            if self.fast_forward:
                machine_duration.setIgnoreToTrue()
            result_durations.addDuration(machine_duration)

            current_time = recovery_time
            last_recover_time = current_time
            if current_time >= end_time - (1E-5):
                break

    # def toString(self):
    #     full_name = super(Machine, self).toString()
    #     parts = full_name.split(".")
    #     return ".".join(parts[2:])
