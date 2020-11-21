from numpy import isnan, isinf

from simulator.Duration import Duration
from simulator.unit.Disk import Disk
from simulator.unit.Sector import Sector


class DiskWithScrubbing(Disk):

    def __init__(self, name, parent, parameters):
        super(DiskWithScrubbing, self).__init__(name, parent, parameters)
        self.latent_error_generator = None
        self.scrub_generator = None
        self.last_recovery_time = 0.0

    def addEventGenerator(self, generator):
        if generator.getName() == "latentErrorGenerator":
            self.latent_error_generator = generator
        elif generator.getName() == "scrubGenerator":
            self.scrub_generator = generator
        else:
            super(DiskWithScrubbing, self).addEventGenerator(generator)

    def getEventGenerators(self):
        return [self.failure_generator, self.recovery_generator, self.latent_error_generator, self.scrub_generator]

    def generateDurations(self, result_durations, start_time, end_time, reset):
        if start_time < self.start_time:
            start_time = self.start_time
        if isnan(start_time) or isinf(start_time):
            raise Exception("start_time = Inf or NAN")
        if isnan(end_time) or isinf(end_time):
            raise Exception("end_time = Inf or NAN")

        current_time = start_time

        if start_time == 0:
            self.last_recovery_time = 0
            self.latent_error_generator.reset(0)

        while True:
            if self.last_recovery_time < 0:
                raise Exception("Negative last recover time")

            # The loop below is what makes the difference for avoiding weird
            # amplification of failures when having machine failures.
            # The reason is as follows: when generateDurations is called once for
            # the whole duration of the simulation(as when there are no
            # machine failures), this loop will never be executed. But when
            # machine fail, the function is called for the time interval
            # between machine recovery and second failure. The first time
            # the disk failure event generated, it may occur after the machine
            # failure event, so it is discarded when it is called for the next
            # time interval, the new failure event might be generated, to be
            # before the current start of the current interval. It's tempting
            # to round that event to the start of the interval, but then it
            # occurs concurrently to many disks. So the critical addition is
            # this loop, which effectively forces the proper generation of the
            # event, which is consistent with the previously generated one that
            # was discarded.
            failure_time = 0
            failure_time = self.failure_generator.generateNextEvent(
                self.last_recovery_time)
            while failure_time < start_time:
                failure_time = self.failure_generator.generateNextEvent(
                    self.last_recovery_time)

            if failure_time > end_time:
                self.generateLatentErrors(result_durations, current_time,
                                          end_time)
                break

            if failure_time < start_time or failure_time > end_time:
                raise Exception("Wrong time range.")

            recovery_time = self.generateRecoveryTime(failure_time, end_time)

            disk_duration = Duration(Duration.DurationType.Loss, failure_time, recovery_time, self)
            result_durations.addDuration(disk_duration)

            # generate latent errors from the current time to the time of the
            # generated failure.
            self.generateLatentErrors(result_durations, current_time,
                                      failure_time)

            # lifetime of a latent error starts when the disk is reconstructed
            self.latent_error_generator.reset(recovery_time)

            # move the clocks, next iteration starts from the next recovery
            current_time = self.last_recovery_time
            if current_time < 0:
                raise Exception("current recovery time is negative")

    def generateRecoveryTime(self, failure_time, end_time):
        if end_time < 0 or failure_time < 0:
            raise Exception("end time or failure time is negative")
        if isinf(failure_time) or isnan(failure_time):
            raise Exception("start time = Inf or NAN")
        if isinf(end_time) or isnan(end_time):
            raise Exception("end time = Inf or NaN")

        self.recovery_generator.reset(failure_time)
        recovery_time = self.recovery_generator.generateNextEvent(failure_time)
        # only failure identification time included in recovery_generator, data transfer time must be added
        recovery_time += self.disk_repair_time

        # if recovery falls later than the end time (which is the time of the
        # next failure of the higher-level component we just co-locate the
        # recovery with the failure because the data will remain unavailable
        # in either case)
        if recovery_time > end_time:
            recovery_time = end_time
        self.last_recovery_time = recovery_time
        if self.last_recovery_time < 0:
            raise Exception("recovery time is negative")
        return recovery_time

    # generate latent sector error and scrub repair duration
    def generateLatentErrors(self, result_durations, start_time, end_time):
        if isinf(start_time) or isnan(start_time):
            raise Exception("start time = Inf or NAN")
        if isinf(end_time) or isnan(end_time):
            raise Exception("end time = Inf or NaN")

        current_time = start_time
        while True:
            latent_error_time = self.latent_error_generator.generateNextEvent(
                current_time)
            if isinf(latent_error_time):
                break
            if isinf(current_time) or isnan(current_time):
                raise Exception("current time is infinitiy or -infinitiy")
            if isinf(latent_error_time) or isnan(latent_error_time):
                raise Exception("current time is infinitiy or -infinitiy")

            current_time = latent_error_time
            if current_time > end_time:
                break

            scrub_recovery_time = self.scrub_generator.generateNextEvent(current_time)

            # LSE on the disk
            failure_type = 0
            LSE_duration = Duration(Duration.DurationType.Loss, latent_error_time,
                    scrub_recovery_time, Sector("sector", self), failure_type)
            result_durations.addDuration(LSE_duration)

            current_time = scrub_recovery_time
            if current_time > end_time:
                break
            self.scrub_generator.reset(current_time)


