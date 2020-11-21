import os
import sys
import csv

from random import uniform, sample
from copy import deepcopy
from time import strftime

from simulator.Duration import Duration
from simulator.Result import Result
from simulator.utils import splitMethod
from simulator.DurationQueue import DurationQueue
from simulator.Log import info_logger, error_logger
from simulator.Configuration import Configuration
from simulator.XMLParser import XMLParser
from simulator.HandleDuration import HandleDuration

from simulator.unit.Rack import Rack
from simulator.unit.Machine import Machine
from simulator.unit.Disk import Disk

from simulator.dataDistribute.SSSDistribute import SSSDistribute, HierSSSDistribute

DEFAULT = r"/root/SIMDDC/conf/"
RESULT = r"/root/SIMDDC/log/"


class Simulation(object):

    def __init__(self, conf_path):
        self.iteration_times = 1
        self.ts = strftime("%Y%m%d.%H.%M.%S")
        self.total_events_handled = 0

        self.conf = Configuration(conf_path)
        xml = XMLParser(self.conf)
        if self.conf.hier:
            self.distributer = HierSSSDistribute(xml)
        else:
            self.distributer = SSSDistribute(xml)
        # self.conf = self.distributer.returnConf()

    def insertUpgradeDurations(self, upgrade_infos):
        freq = int(upgrade_infos[0])
        domain = upgrade_infos[1]
        unit_num, unit = splitMethod(domain, '_')
        num = int(unit_num)

        upgrade_durations = []

        num_of_rollings = int(self.conf.total_time)/freq
        start_ts = round(uniform(0, float(self.conf.total_time)%freq), 2)
        for j in xrange(1, num_of_rollings+1):
            rolling_start = start_ts + j*freq

            if unit == "rack":
                loops = self.conf.rack_count / num
                remainder = self.conf.rack_count % num
                racks = self.distributer.getAllRacks()
                for i in xrange(loops):
                    start_time = rolling_start + downtime*i
                    duration = Duration(Duration.DurationType.Unavailable, start_time,
                            start_time+downtime, racks[i*num, (i+1)*num])
                    upgrade_durations.append(duration)
                if remainder != 0:
                    start_time = rolling_start + downtime*loops
                    duration = Duration(Duration.DurationType.Unavailable, start_time,
                            start_time+downtime, racks[-remainder:])
                    upgrade_durations.append(duration)
            elif unit == "machine":
                pass
            else:
                raise Exception("Upgrade domain must be in rack or machine")

        if len(upgrade_infos) == 6:
            self.insertHardUpgrades(upgrade_infos)
        if len(upgrade_infos) == 4:
            self.insertSoftUpgrades(soft_infos)

    def insertCorrelatedFailures(self):
        pass

    def _failedComponents(self, info, interval):
        info.strip()
        scope = splitMethod(info, '_')
        failure_components = []
        failed_amounts = []

        length = len(scope)/2
        for i in xrange(length):
            failed_amounts.append(scope[2*i:2*(i+1)])

        # racks come from system tree, so we can not modify it.
        racks = self.distributer.getAllRacks()
        for num, component in failed_amounts:
            if component == "rack":
                failed_racks = sample(racks, int(num))
                for rack in failed_racks:
                    rack.addFailureInterval(interval)
                    failure_components.append(rack)
            elif component == "machine":
                machines = []
                for rack in racks:
                    if rack in failure_components:
                        continue
                    else:
                        machines += rack.getChildren()
                failed_machines = sample(machines, int(num))
                for machine in failed_machines:
                    machines.remove(machine)
                    machine.addFailureInterval(interval)
                    failure_components.append(machine)
            elif component == "disk":
                disks = []
                for rack in racks:
                    if rack in failure_components:
                        continue
                    rack_disks = []
                    self.distributer.getAllDisksInRack(rack, rack_disks)
                    disks += rack_disks
                failed_disks = sample(disks, int(num))
                for disk in failed_disks:
                    disk.addFailureInterval(interval)
                    failure_components.append(disk)

        return failure_components

    def getDistributer(self):
        return self.distributer

    def writeToCSV(self, res_file_path, contents):
        with open(res_file_path, "w") as fp:
            writer = csv.writer(fp, lineterminator='\n')
            for item in contents:
                writer.writerow(item)

    def run(self):
        root = self.distributer.getRoot()

        self.distributer.distributeSlices(root, conf.total_slices)
        durations_handled = 0
        durations = DurationQueue()

        root.generateDurations(durations, 0, self.conf.total_time, True)

        duration_file = self.conf.event_file + '-' + self.ts
        durations.printAll(duration_file, "Iteration number: "+str(self.iteration_times))

        duration_handler = HandleDuration(durations)

        lost_concurrent, concurrent = duration_handler.findConcurrent()
        lost_times, lost_slice_count, _null, _null1 = duration_handler.process(lost_concurrent, self.distributer)
        Result.lost_slice_count = lost_slice_count
        Result.PDL = format(float(lost_slice_count)/self.conf.total_slices, ".4e")
        Result.PDLT = format(float(lost_slice_count)/self.conf.total_slices, ".4e")
        print "Lost: "+ str(Result.lost_slice_count) + " PDL:" + Result.PDL + " PDLT:" + Result.PDLT

        _null, _null1, unavailable_period, unavailable_period_with_weight = duration_handler.process(concurrent, self.distributer)
        Result.unavailable_slice_count = duration_handler.returnFailureSliceCount()
        Result.PUA = format(unavailable_period/conf.total_time, ".4e")
        Result.PUAW = format(unavailable_period_with_weight/(conf.total_time*conf.total_slices), ".4e")

        print "Unavailable: " + str(Result.unavailable_slice_count) + \
                " PUA:" + Result.PUA + "  PUAW:" + Result.PUAW


    def main(self, num_iterations):
        contents = []

        for i in xrange(num_iterations):
            result = self.run()
            contents.append([result.PDL, result.NOMDL, result.MTTR, result.MTBF, result.PUA, result.PUS, result.TRT])
            unavailable_slices = result.unavailable_slice_durations.keys()
            for slice_index in unavailable_slices:
                print "slice %d unavailable duration %s" % (slice_index, str(result.unavailable_slice_durations[slice_index]))

        res_file_path = RESULT + self.conf.data_redundancy + '-'
        if self.conf.system_upgrade:
            res_file_path += "upgrade-"
        res_file_path += self.ts + ".csv"
        self.writeToCSV(res_file_path, contents)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise Exception("Usage: python Test.py conf_path num_iterations")
    path = sys.argv[1]
    num_iterations = int(sys.argv[2])

    if not os.path.isabs(path):
        conf_path = DEFAULT + path
    else:
        conf_path = path

    sim = Simulation(conf_path)
    sim.run()
