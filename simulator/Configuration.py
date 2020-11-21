import ConfigParser
import os
from math import ceil, floor
from random import random

from simulator.Log import info_logger
from simulator.drs.Handler import getDRSHandler
from simulator.utils import splitMethod, splitIntMethod, splitFloatMethod, \
    extractDRS, returnEventGenerator

BASE_PATH = r"/root/SIMDDC/"
CONF_PATH = BASE_PATH + "conf/"


def getConfParser(conf_file_path):
    conf = ConfigParser.ConfigParser()
    conf.read(conf_file_path)
    return conf


class Configuration(object):
    path = CONF_PATH + "simddc.conf"

    def __init__(self, path=None):
        if path is None:
            conf_path = Configuration.path
        else:
            conf_path = path
        self.conf = getConfParser(conf_path)

        try:
            d = self.conf.defaults()
        except ConfigParser.NoSectionError:
            raise Exception("No Default Section!")

        self.total_time = int(d["total_time"])
        # total active storage in PBs
        self.total_active_storage = float(d["total_active_storage"])

        self.chunk_size = int(d["chunk_size"])
        self.disk_capacity = float(d["disk_capacity"])
        # tanslate TB of manufacturrer(10^12 bytes) into GBs(2^30 bytes)
        self.actual_disk_capacity = self.disk_capacity * pow(10, 12)/ pow(2, 30)
        self.max_chunks_per_disk = floor(self.actual_disk_capacity*1024/self.chunk_size)
        self.disks_per_machine = int(d["disks_per_machine"])
        self.machines_per_rack = int(d["machines_per_rack"])
        self.rack_count = int(d["rack_count"])
        self.datacenters = 1

        self.xml_file_path = d.pop("xml_file_path")
        self.event_file = d.pop("event_file", None)

        self.data_redundancy = d["data_redundancy"]
        data_redundancy = extractDRS(self.data_redundancy)

        self.recovery_bandwidth_cross_rack = int(d["recovery_bandwidth_cross_rack"])
        self.node_bandwidth = int(d["node_bandwidth"])

        self.parallel_repair = self._bool(d.pop("parallel_repair", "false"))
        self.drs_handler = getDRSHandler(data_redundancy[0], data_redundancy[1:])

        # flat or hier, if hier, r is the distinct_racks
        self.hier = self._bool(d.pop("hierarchical", "false"))
        if self.hier:
            self.r = int(d["distinct_racks"])
        else:
            self.r = self.drs_handler.n

        self.upgrades = False
        self.failure_generator = None
        self.lse_generator = None
        self.hard_upgrade_infos = None
        if self.conf.has_section("Hard Upgrades"):
            self.upgrades = True
            freq = self.conf.getint("Hard Upgrades", "freq")
            domain = self.conf.get("Hard Upgrades", "domain")
            repair_after_upgrades = self.conf.getboolean("Hard Upgrades", "repair_after_upgrades")
            if repair_after_upgrades:
                bandwidth_usage = 1
            else:
                bandwidth_usage = self.conf.getfloat("Hard Upgrades", "bandwidth_usage")

            if self.conf.has_option("Hard Upgrades", "disk_failure_generator"):
                disk_failure_generator = self.conf.get("Hard Upgrades", "disk_failure_generator")
                self.failure_generator = returnEventGenerator("failureGenerator", disk_failure_generator)
            if self.conf.has_option("Hard Upgrades", "latent_error_generator"):
                latent_error_generator = self.conf.get("Hard Upgrades", "latent_error_generator")
                self.lse_generator = returnEventGenerator("latentErrorGenerator", latent_error_generator)

            self.hard_upgrade_infos = (freq, domain, repair_after_upgrades, bandwidth_usage,
                    disk_failure_generator, latent_error_generator)

        if self.conf.has_section("Soft Upgrades"):
            self.upgrades = True
            freq = self.conf.getint("Soft Upgrades", "freq")
            domain = self.conf.get("Soft Upgrades", "domain")
            downtime = self.conf.getint("Soft Upgrades", "downtime")
            check_style = self.conf.getint("Soft Upgrades", "check_style")
            self.soft_upgrade_infos = (freq, domain, downtime, check_style)

        self.correlated_failures = False
        self.correlated_failures_infos = []
        all_sections = self.conf.sections()
        sections = []
        for item in all_sections:
            if item.startswith("Correlated Failures"):
                sections.append(item)

        if sections != []:
            self.correlated_failures = True
            for section in sections:
                self.correlated_failures_infos.append(self.parserCorrelatedSetting(section))

        self.disk_repair_time, self.node_repair_time = self.comRepairTime()

        self.total_slices = int(ceil(self.total_active_storage*pow(2,30)/(self.drs_handler.k*self.chunk_size)))

    def _bool(self, string):
        if string.lower() == "true":
            return True
        elif string.lower() == "false":
            return False
        else:
            raise Exception("String must be 'true' or 'false'!")

    def _checkSpace(self):
        total_system_capacity = self.actual_disk_capacity * self.disks_per_machine * self.machines_per_rack * self.rack_count
        min_necess_capacity = float(self.total_active_storage) * self.drs_handler.n / self.drs_handler.k
        if min_necess_capacity >= total_system_capacity:
            raise Exception("Not have enough space!")

    def getDRSHandler(self):
        return self.drs_handler

    def comRepairTime(self):
        repair_traffic = self.drs_handler.repairTraffic(self.hier, self.r)
        # in MB/s
        aggregate_bandwidth = self.recovery_bandwidth_cross_rack * self.rack_count

        # used disk space in MBs
        used_disk_space = self.drs_handler.SO*self.total_active_storage*pow(2,30)/(self.rack_count*self.machines_per_rack*self.disks_per_machine)

        # repair time in hours
        disk_repair_time = round(repair_traffic*used_disk_space/aggregate_bandwidth, 2)
        node_repair_time = disk_repair_time*self.disks_per_machine

        return disk_repair_time, node_repair_time

    # "True" means events record to file, and vice versa.
    def eventToFile(self):
        return self.event_file is not None

    def parserCorrelatedSetting(self, section_name):
        component = self.conf.get(section_name, "component")
        failure_rate = self.conf.getfloat(section_name, "failure_rate")

        return (component, failure_rate)

    def returnSliceSize(self):
        return self.chunk_size * self.drs_handler.n

    def returnAll(self):
        d = {"total_time": self.total_time,
             "total_active_storage": self.total_active_storage,
             "chunk_size": self.chunk_size,
             "disk_capacity": self.disk_capacity,
             "disks_per_machine": self.disks_per_machine,
             "machines_per_rack": self.machines_per_rack,
             "xml_file_path": self.xml_file_path,
             "event_file": self.event_file,
             "data_redundancy": self.data_redundancy,
             "hierarchical": self.hier,
             "recovery_bandwidth_cross_rack": self.recovery_bandwidth_cross_rack,
             "parallel_repair": self.parallel_repair,
             "upgrades": self.upgrades,
             "correlated_failures": self.correlated_failures}
        if self.hier:
            d["distinct_racks"] = self.r

        if self.upgrades:
            d["hard_upgrade_infos"] = self.hard_upgrade_infos
            d["soft_upgrade_infos"] = self.soft_upgrade_infos
        if self.correlated_failures:
            d["correlated_failures"] = self.correlated_failures_infos

        return d

    def printTest(self):
        d = self.returnAll()
        keys = d.keys()
        for key in keys:
            print key, d[key]

    def printAll(self):
        default_infos = "Default Configurations: \t total_time: " + str(self.total_time) + \
                        ", disk capacity: " + str(self.disk_capacity) + "TB" + \
                        ", disks per machine: " + str(self.disks_per_machine) + \
                        ", machines per rack: " + str(self.machines_per_rack) + \
                        ", rack count: " + str(self.rack_count) + \
                        ", chunk size: " + str(self.chunk_size) + "MB" + \
                        ", total active storage: " + str(self.total_active_storage) + "PB" +\
                        ", data redundancy: " + str(self.data_redundancy) + \
                        ", hierarchical:" + str(self.hier) + \
                        ", recovery bandwidth cross rack: " + str(self.recovery_bandwidth_cross_rack) + \
                        ", xml file path: " + self.xml_file_path + \
                        ", event file path: " + self.event_file + \
                        ", parallel repair: " + str(self.parallel_repair) + \
                        ", upgrade flag: " + str(self.upgrades) + \
                        ", correlated failures flag: " + str(self.correlated_failures)

        info_logger.info(default_infos)

        if self.upgrades:
            info_logger.info("Upgrade Configurations: " + str(self.hard_upgrade_infos))
            info_logger.info("Upgrade Configurations: " + str(self.soft_upgrade_infos))

        if self.correlated_failures:
            info_logger.info("Correlated Failures Configurations: " + str(self.correlated_failures_infos))


if __name__ == "__main__":
    conf = Configuration("/root/SIMDDC/conf/simddc.conf")
    drs_handler = conf.getDRSHandler()
    conf.printTest()
    conf.printAll()
    print conf.failure_generator
    print conf.lse_generator
    print conf.disk_repair_time, conf.node_repair_time
