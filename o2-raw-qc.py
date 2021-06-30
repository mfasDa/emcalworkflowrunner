#! /usr/bin/env python3

import argparse
import os
import subprocess

from o2framework.FairMQChannel import FairMQChannel

class dpl:

    def __init__(self):
        self._executable: str = ""

    def build(self, defaultsession: bool, batchmode: bool) -> str:
        cmd = self._executable
        if batchmode:
            cmd += " -b"
        if defaultsession:
            cmd += " --session default" 
        cmd += " {}".format(self._build_args())
        return cmd

    def _build_args(self) -> str():
        return ""

class rawproxy(dpl):

    def __init__(self, detector: str, channel: FairMQChannel):
        super().__init__()
        self._executable = "o2-dpl-raw-proxy"
        self.__detector = detector
        self.__channel = channel

    def _build_args(self):
        return "--dataspec \"A:{}/RAWDATA\" --channel-config \"{}\"".format(self.__detector, self.__channel)

class qcrunner(dpl):

    def __init__(self, configfile: str):
        super().__init__()
        self._executable = "o2-qc"
        self.__config = configfile

    def _build_args(self) -> str():
        return "--config json://{}".format(os.path.abspath(self.__config))
        

class workflow:

    def __init__(self, defaultsession: bool = True, batchmode: bool = True):
        self.__processors = []
        self.__defaultsession = defaultsession
        self.__batchmode = batchmode

    def add_processor(self, processor: dpl):
        self.__processors.append(processor)

    def run(self):
        cmd = ""
        for proc in self.__processors:
            if len(cmd):
                cmd += " | "
            cmd += proc.build(self.__defaultsession, self.__batchmode)
        print("Running {}".format(cmd))
        subprocess.call(cmd, shell=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser("o2-raw-reader.py", description="Launch handler for raw reader workflow")
    parser.add_argument("configfile", metavar="CONFIGFILE", type=str, help="Input directory with raw files")
    parser.add_argument("-b", "--batchmode", action="store_true", help="Batch mode (no debug GUI)")
    parser.add_argument("-d", "--detector", metavar="DETECTOR", type=str, default="EMC", help="Detector string")
    parser.add_argument("--fmqname", metavar="FMQCHANNELNAME", type=str, default="", help="Name of the FairMQ channel")
    parser.add_argument("--fmqaddress", metavar="FMQCHANNELADDRESS", type=str, default="", help="Address of the FairMQ channel")
    parser.add_argument("--fmqtransport", metavar="FMQCHANNELTRANSPORT", type=str, default="", help="Transport type of the FairMQ channel")
    args = parser.parse_args()

    channel = FairMQChannel()
    channel.channeltype = "pull"
    channel.method = "connect"
    if len(args.fmqname):
        channel.name = args.fmqname
    if len(args.fmqaddress):
        channel.addresss = args.fmqaddress
    if len(args.fmqtransport):
        channel.transport = args.fmqtransport

    fullworkflow = workflow(batchmode=args.batchmode)
    fullworkflow.add_processor(rawproxy(args.detector, channel))
    fullworkflow.add_processor(qcrunner(args.configfile))
    fullworkflow.run()