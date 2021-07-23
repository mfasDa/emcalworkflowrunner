#! /usr/bin/env python3

import argparse
import os
import subprocess

from o2framework.FairMQChannel import FairMQChannel

class RawFile:

    def __init__(self, path: str, detector: str, datatype: str):
        self.__path = path
        self.__detector = detector
        self.__datatye = datatype
        self.__card_type = "RORC"

    def __eq__(self, other):
        if not isinstance(other, RawFile):
            return False
        return self.__path == other.get_path()

    def __lt__(self, other):
        if not isinstance(other, RawFile):
            return False
        return self.__path < other.get_path()

    def __cmp__(self, other):
        if not isinstance(other, RawFile):
            return -1
        otherpath = other.get_path()
        if self.__path == otherpath:
            return 0
        elif self.__path < otherpath:
            return -1
        else:
            return 1

    def get_path(self) -> str:
        return self.__path

    def get_detector(self) -> str:
        return self.__detector

    def get_datatype(self) -> str:
        return self.__datatye

    def get_card_type(self) -> str:
        return self.__card_type

    def write_configuration(self, writer):
        writer.write("dataOrigin = {}\n".format(self.__detector))
        writer.write("dataDescription = {}\n".format(self.__datatye))
        writer.write("filePath = {}\n".format(self.__path))
        writer.write("readoutCard = {}\n".format(self.__card_type))


class DataCollection:

    def __init__(self, inputdir: str, detector: str, datatype):
        self.__rawfiles = self.__scan(inputdir, detector, datatype)
        self.__detector = detector
        self.__datatype = datatype

    def __scan(self, inputdir: str, detector: str, datatype: str) -> list:
        result = []
        inputfiles = [x for x in os.listdir(inputdir) if "raw" in x]
        for inputfile in inputfiles:
            result.append(RawFile(os.path.join(inputdir, inputfile), detector, datatype))
        return result

    def write_configuration(self, configfile: str):
        with open(configfile, "w") as configwriter:
            configwriter.write("[defaults]\n")
            configwriter.write("dataOrigin = {}\n".format(self.__detector))
            configwriter.write("dataDescription = {}\n".format(self.__datatype))

            currentindex = 0
            for rawfl in sorted(self.__rawfiles):
                configwriter.write("[input-{}]\n".format(currentindex))
                currentindex += 1
                rawfl.write_configuration(configwriter)

            configwriter.close()


class RawReader:

    def __init__(self, configfile: str, channel: FairMQChannel, useDefaultSession: bool = True, batchmode: bool = False, loop: int = 1000, delay: int = 3, shmSegmentSize = 16000000000):
        self.__configfile = configfile
        self.__channel = channel
        self.__useDefaultSession = useDefaultSession
        self.__batchmode = batchmode
        self.__loop = loop
        self.__delay = delay
        self.__shmSegmentSize = shmSegmentSize

    def run(self):
        cmd = "o2-raw-file-reader-workflow"
        if self.__batchmode:
            cmd += " -b"
        if self.__useDefaultSession:
            cmd += " --session default "
        cmd += " --loop {}".format(self.__loop)
        cmd += " --delay {}".format(self.__delay)
        cmd += " --input-conf {}".format(self.__configfile)
        cmd += " --raw-channel-config \"{}\"".format(self.__channel)
        cmd += " --shm-segment-size {}".format(self.__shmSegmentSize)
        print("Running command: {}".format(cmd))
        subprocess.call(cmd, shell=True)

if __name__ == "__main__":
    print("In main")
    parser = argparse.ArgumentParser("o2-raw-reader.py", description="Launch handler for raw reader workflow")
    parser.add_argument("inputdir", metavar="INPUTDIR", type=str, help="Input directory with raw files")
    parser.add_argument("-b", "--batchmode", action="store_true", help="Batch mode (no debug GUI)")
    parser.add_argument("-d", "--detector", metavar="DETECTOR", type=str, default="EMC", help="Detector string")
    parser.add_argument("--fmqname", metavar="FMQCHANNELNAME", type=str, default="", help="Name of the FairMQ channel")
    parser.add_argument("--fmqaddress", metavar="FMQCHANNELADDRESS", type=str, default="", help="Address of the FairMQ channel")
    parser.add_argument("--fmqtransport", metavar="FMQCHANNELTRANSPORT", type=str, default="", help="Transport type of the FairMQ channel")
    args = parser.parse_args()

    channel = FairMQChannel()
    channel.channeltype = "push"
    channel.method = "bind"
    if len(args.fmqname):
        channel.name = args.fmqname
    if len(args.fmqaddress):
        channel.addresss = args.fmqaddress
    if len(args.fmqtransport):
        channel.transport = args.fmqtransport

    print("using channel: {}".format(channel))

    # creating rawreader.cfg
    rawcfgfile = "rawreader.cfg"
    if os.path.exists(rawcfgfile):
        os.remove(rawcfgfile)
    rawconfig = DataCollection(os.path.abspath(args.inputdir), args.detector, "RAWDATA")
    rawconfig.write_configuration(rawcfgfile)

    runner = RawReader(rawcfgfile, channel, batchmode=args.batchmode)
    runner.run()

