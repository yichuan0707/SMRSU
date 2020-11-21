from math import floor
from random import randint, sample, choice

from simulator.Configuration import Configuration
from simulator.XMLParser import XMLParser
from simulator.dataDistribute.base import DataDistribute
from simulator.Log import error_logger


class SSSDistribute(DataDistribute):
    """
    SSS: Spread placement Strategy System.
    All stripes of one file randomly spread, so the file spreads more than n disks.
    """

    def distributeSlices(self, root, total_slices):
        disks = []

        self.getAllDisks(root, disks)
        for i in xrange(total_slices):
            self.slice_locations.append([])
            tmp_racks = [item for item in disks]
            for j in xrange(self.n):
                self.distributeSliceToDisk(i, disks, tmp_racks)

            self._my_assert(len(tmp_racks) == (self.conf.rack_count - self.n))
            self._my_assert(len(self.slice_locations[i]) == self.n)

        self._my_assert(len(self.slice_locations) == total_slices)

    def distributeSliceToDisk(self, slice_index, disks, available_racks):
        retry_count = 0
        same_rack_count = 0
        same_disk_count = 0
        full_disk_count = 0
        while True:
            retry_count += 1
            # choose disk from the right rack
            if len(available_racks) == 0:
                raise Exception("No racks left")
            prev_racks_index = randint(0, len(available_racks)-1)
            rack_disks = available_racks[prev_racks_index]

            disk_index_in_rack = randint(0, len(rack_disks)-1)
            disk = rack_disks[disk_index_in_rack]
            slice_count = len(disk.getChildren())
            if slice_count >= self.conf.max_chunks_per_disk:
                full_disk_count += 1
                rack_disks.remove(disk)

                if len(rack_disks) == 0:
                    error_logger.error("One rack is completely full" + str(disk.getParent().getParent().getID()))
                    available_racks.remove(rack_disks)
                    disks.remove(rack_disks)
                if retry_count > 100:
                    error_logger.error("Unable to distribute slice " + str(slice_index) + "; picked full disk " +
                                       str(full_disk_count) + " times, same rack " + str(same_rack_count) +
                                       " times, and same disk " + str(same_disk_count) + " times")
                    raise Exception("Disk distribution failed")
                continue

            available_racks.remove(rack_disks)

            # LZR
            self.slice_locations[slice_index].append(disk)

            # add slice indexs to children list of disks
            disk.addChild(slice_index)
            break


class HierSSSDistribute(SSSDistribute):
    """
    Hierarchical + SSS dataplacement
    """
    def __init__(self, xml):
        super(HierSSSDistribute, self).__init__(xml)
        self.r = self.conf.r
        slice_chunks_on_each_rack = self.n/self.r
        if slice_chunks_on_each_rack == 0:
            raise Exception("Distinct_racks is too large")
        self.slices_chunks_on_racks = [slice_chunks_on_each_rack] * self.r
        if self.n % self.r:
            for i in xrange(self.n % self.r):
                self.slices_chunks_on_racks[i] += 1

    def distributeSlices(self, root, increase_slices):
        machines = self.getAllMachines()
        self.total_slices += increase_slices
        for slice_index in xrange(self.total_slices - increase_slices, self.total_slices):
            self.slice_locations.append([])
            self.distributeSliceToDisk(slice_index, machines)

            self._my_assert(len(self.slice_locations[slice_index]) == self.n)

        self._my_assert(len(self.slice_locations) == self.total_slices)

    def distributeSliceToDisk(self, slice_index, machines):
        retry_count = 0
        full_machine_count = 0
        locations = []

        if len(machines) < self.r:
            raise Exception("No enough racks left")

        retry_flag = True
        while retry_flag:
            racks_for_slice = sample(machines, self.r)

            retry_flag = False
            for i, rack in enumerate(racks_for_slice):
                if len(rack) < self.slices_chunks_on_racks[i]:
                    retry_flag = True
                    retry_count += 1
                    if retry_count > 100:
                        error_logger.error("Unable to distribute slice " + str(slice_index))
                        raise Exception("Data distribution failed")
                    else:
                        break

        # choose machines from the right rack
        for i, rack in enumerate(racks_for_slice):
            machines_for_slice = sample(rack, self.slices_chunks_on_racks[i])
            for machine in machines_for_slice:
                disk = choice(machine.getChildren())
                locations.append(disk)
                disk.addChild(slice_index)
                slice_count = len(disk.getChildren())
                if slice_count >= self.conf.max_chunks_per_disk:
                    full_disk_count += 1
                    error_logger.info("One disk is completely full " + str(disk.toString()))
                    rack.remove(machine)

                if len(rack) == 0:
                    error_logger.error("One rack is completely full" + str(machine.getParent().getID()))
                    machines.remove(rack)
        # LZR
        self.slice_locations[slice_index] = locations


if __name__ == "__main__":
    conf = Configuration()
    xml = XMLParser(conf)
    sss = SSSDistribute(xml)
    print "disk usage is: ", sss.diskUsage()
    print "second print machines:", sss.getAllMachines()
    sss.start()
    sss.printTest()
    sss.printToFile()
    # sss.printTest()
    # sss.printToFile()
    print "disk usage is: ", sss.diskUsage()

