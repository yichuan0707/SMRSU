from time import strftime, time

from simulator.Configuration import Configuration
from simulator.Log import error_logger
from simulator.XMLParser import XMLParser
from simulator.unit.Rack import Rack


class DataDistribute(object):

    def __init__(self, xml):
        self.xml = xml
        self.conf = self.xml.config
        self.n = self.conf.drs_handler.n
        self.k = self.conf.drs_handler.k

        # scaling up and heteogeneous layer need XMLParser module supports rebuild.
        units = self.xml.readFile()
        self.root = units[0]

        self.slice_locations = []

        # groups of pss and copyset data placement
        self.groups = None
        self.conf.printAll()

    def _my_assert(self, expression):
        if not expression:
            raise Exception("Assertion Failed!")
        return True

    def returnConf(self):
        return self.conf

    def returnCodingParameters(self):
        return (self.n, self.k)

    def returnSliceLocations(self):
        return self.slice_locations

    def getRoot(self):
        return self.root

    def getGroups(self):
        return self.groups

    def diskUsage(self):
        total_disk_count = self.conf.rack_count * self.conf.machines_per_rack * self.conf.disks_per_machine
        disk_cap_in_MBs = self.conf.disk_capacity * pow(10,12)/pow(2,20)
        system_cap = total_disk_count * disk_cap_in_MBs
        return self.conf.total_slices*self.n*self.conf.chunk_size/system_cap

    def distributeSlices(self, root, total_slices):
        pass

    def getAllRacks(self):
        r = self.root
        if not isinstance(r.getChildren()[0], Rack):
            r = r.getChildren()[0]

        return r.getChildren()

    def getAllMachines(self):
        machines = []

        racks = self.getAllRacks()
        for rack in racks:
            machines.append(rack.getChildren())

        return machines

    def getAllDisks(self, u, disks):
        for tmp in u.getChildren():
            if isinstance(tmp, Rack):
                rack_disks = []
                self.getAllDisksInRack(tmp, rack_disks)
                disks.append(rack_disks)
            else:
                self.getAllDisks(tmp, disks)

    def getAllDisksInRack(self, u, disks):
        for tmp in u.getChildren():
            for m in tmp.getChildren():
                disks.append(m)

    def printToFile(self, file_path=r"/root/SIMDDC/log/slices_distribution"):
        ts = strftime("%Y%m%d.%H.%M.%S")
        file_path += '-' + ts
        with open(file_path, 'w') as fp:
            for i, item in enumerate(self.slice_locations):
                info = "slice " + str(i) + ": "
                for d in item:
                    info += d.toString() + ", "
                info += "\n"
                fp.write(info)


if __name__ == "__main__":
    pass

