#! /usr/bin/env python3

class FairMQChannel:

    def __init__(self, name: str = "readout-proxy", channeltype: str = "push", method: str = "bind", address: str = "ipc:///tmp/readout-pipe-1", transport: str = "shmem", ratelogging: int = 1):
        self.__name = name
        self.__channeltype = channeltype
        self.__method = method
        self.__address= address
        self.__transport = transport
        self.__rateLogging = ratelogging

    def set_name(self, name: str):
        self.__name = name

    def set_channeltype(self, chantype: str):
        self.__channeltype = chantype

    def set_method(self, method: str):
        self.__method = method

    def set_address(self, address: str):
        self.__address = address

    def set_transport(self, transport: str):
        self.__transport = transport

    def set_rateLogging(self, rateLogging: int):
        self.__rateLogging = rateLogging

    def get_name(self) -> str:
        return self.__name

    def get_channeltype(self) -> str:
        return self.__channeltype

    def get_method(self) -> str:
        return self.__method

    def get_address(self) -> str:
        return self.__address

    def get_transport(self) -> str:
        return self.__transport

    def get_rateLogging(self) ->int:
        return self.__rateLogging

    name = property(fget=get_name, fset=set_name)
    channeltype = property(fget=get_channeltype, fset=set_channeltype)
    method = property(fget=get_method, fset=set_method)
    addresss = property(fget=get_address, fset=set_address)
    transport = property(fget=get_transport, fset=set_transport)
    rateLogging = property(fget=get_rateLogging, fset=set_rateLogging)

    def __str__(self):
        return "name={},type={},method={},address={},transport={},rateLogging={}".format(self.__name, self.__channeltype, self.__method, self.__address, self.__transport, self.__rateLogging)