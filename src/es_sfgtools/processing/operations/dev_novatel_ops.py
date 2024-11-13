"""
  @file   GPSABinaryExtractor.py
  @brief  Class to extract data stored within a GPSA binary format

  Copyright:   Sonardyne (C) 2020

  All rights are reserved. Reproduction or transmission in whole or
  in part, in any form or by any means, electronic, mechanical or
  otherwise, is prohibited without the prior written consent of
  the copyright owner.

  To obtain written consent please contact:

          Software Release Authority
          Sonardyne International Ltd
          Ocean House
          Blackbushe Business Park
          Yateley
          Hampshire. GU46 6GD
          United Kingdom
"""

"""     Global Imports       """
import os
import re
import sys
import struct
import array
import argparse
import logging
from pathlib import Path
from ctypes import c_ubyte, c_uint16, c_uint32, c_uint64
from datetime import datetime, timedelta,timezone


"""
@class  GPSAPacket GPSA Binary Data Packet
@brief  An object contaning a valid binary data packet and some helper
        functions for accessing the data
"""

GNSS_START_TIME = datetime(1980, 1, 6, tzinfo=timezone.utc)  # GNSS start time


class GPSAPacket:
    """Class defaults"""

    buffer = []
    msg_id = 0
    inst_time = 0
    common_time = 0

    """
    @brief  Constructor used to initialise the data structure
    @params msg_id (int) an integer giving the message ID
            inst_time (float) time in milliseconds since system was powered on (Instrument Frame Time)
            common_time (float) time in milliseconds from wall clock (Common Frame Time)
            buffer (list[int]) the data buffer
    """

    def __init__(self, msg_id, inst_time, common_time, buffer):
        self.buffer = buffer
        self.msg_id = int(msg_id.value)
        self.inst_time = float(inst_time.value) * 0.000001
        self.common_time = float(common_time.value) * 0.000001

    """
    @brief  Helper function to get the current Instrument Frame Time
    """

    def get_inst_time(self):
        return self.inst_time

    """
    @brief  Helper function to get the current Common Frame Time
    """

    def get_common_time(self):
        return self.common_time

    """
    @brief  Helper function to get the message ID
    """

    def get_msg_id(self):
        return self.msg_id

    """
    @brief  Helper function to get a shallow copy of the buffer
    """

    def buffer_to_file(self, fh):
        return self.buffer.tofile(fh)


class GPSAPacketExtractor:
    """Class defaults"""

    _dle = 16
    _stx = 2
    _etx = 3

    _got_dle = False
    _got_stx = False
    _buffer = array.array("B")

    _crc = c_ubyte(0)

    def __init__(self):
        pass

    def find_packet(self, byte):

        # if its a DLE then mark that we've seen some stuffing
        if byte == self._dle and self._got_dle == False:
            self._got_dle = True

        # if its an STX following a DLE then mark start of packet
        elif byte == self._stx and self._got_dle == True:
            self._got_dle = False
            self._got_stx = True
            self._buffer = array.array("B")
            self._crc = c_ubyte(0)

        # if its an ETX following a DLE then return the packet
        elif byte == self._etx and self._got_dle == True:
            # Check the packet had a valid STX at the start
            if self._got_stx == False:
                logging.warning("Found a valid ETX with no STX")
                ret = None

            elif self._crc.value != 0:
                logging.warning("CRC Error in packet")
                ret = None

            else:
                msg_id = c_uint16(
                    (
                        (c_ubyte(self._buffer[0]).value << 8)
                        + c_ubyte(self._buffer[1]).value
                    )
                    & c_uint16(1023).value
                )
                inst_time = c_uint64(
                    (c_ubyte(self._buffer[9]).value << 52)
                    + (c_ubyte(self._buffer[8]).value << 48)
                    + (c_ubyte(self._buffer[7]).value << 40)
                    + (c_ubyte(self._buffer[6]).value << 32)
                    + (c_ubyte(self._buffer[5]).value << 24)
                    + (c_ubyte(self._buffer[4]).value << 16)
                    + (c_ubyte(self._buffer[3]).value << 8)
                    + c_ubyte(self._buffer[2]).value
                )
                common_time = c_uint64(
                    (c_ubyte(self._buffer[17]).value << 52)
                    + (c_ubyte(self._buffer[16]).value << 48)
                    + (c_ubyte(self._buffer[15]).value << 40)
                    + (c_ubyte(self._buffer[14]).value << 32)
                    + (c_ubyte(self._buffer[13]).value << 24)
                    + (c_ubyte(self._buffer[12]).value << 16)
                    + (c_ubyte(self._buffer[11]).value << 8)
                    + c_ubyte(self._buffer[10]).value
                )
                ret = GPSAPacket(msg_id, inst_time, common_time, self._buffer[18:-1])

            # Reset everythin before we return
            self._got_dle = False
            self._got_stx = False
            return ret

        # Append all other bytes to the buffer
        else:
            # If the last byte was a DLE and the current one isn't then we're missing a byte
            if self._got_dle == True:
                if byte != self._dle:
                    logging.warning("Found DLE with no stuffing")
                self._got_dle = False

            self._buffer.append(byte)
            self._crc = c_ubyte(self._crc.value ^ c_ubyte(byte).value)

        return None

    # def find_packet(self, byte):

    #     # if its a DLE then mark that we've seen some stuffing
    #     if byte == self._dle and self._got_dle == False:
    #         self._got_dle = True

    #     # if its an STX following a DLE then mark start of packet
    #     elif byte == self._stx and self._got_dle == True:
    #         self._got_dle = False
    #         self._got_stx = True
    #         self._buffer = array.array("B")
    #         self._crc = c_ubyte(0)

    #     # if its an ETX following a DLE then return the packet
    #     elif byte == self._etx and self._got_dle == True:
    #         # Check the packet had a valid STX at the start
    #         if self._got_stx == False:
    #             # logging.warning("Found a valid ETX with no STX")

    #             ret = None

    #         elif self._crc.value != 0:
    #             # logging.warning("CRC Error in packet")

    #             ret = None

    #         else:
    #             ret = self._buffer[18:-1]

    #         self._got_dle = False
    #         self._got_stx = False
    #         return ret

    #     # Append all other bytes to the buffer
    #     else:
    #         # If the last byte was a DLE and the current one isn't then we're missing a byte
    #         if self._got_dle == True:
    #             if byte != self._dle:
    #                 pass
    #                 # logging.warning("Found DLE with no stuffing")
    #             self._got_dle = False

    #         self._buffer.append(byte)
    #         self._crc = c_ubyte(self._crc.value ^ c_ubyte(byte).value)

    #     return None

    """
    @brief  function will iterate over the file list provided to the constructor
            and yield whenever a packet it found
    """

    def get_packet(self,filename: str | Path):
        with open(filename, "rb") as fh:
            for byte in fh.read():
                pkt = self.find_packet(byte)
                if pkt != None:
                    yield pkt


def get_date(filename: str | Path) -> datetime:


    decoder = GPSAPacketExtractor()
    for pkt in decoder.get_packet(filename):
        string_rep = pkt.buffer.tobytes().decode("utf-8", errors="replace")
        if "INSPVAA" in string_rep:
            print(string_rep)
            line = string_rep.split(";")[1].split(",")
            gnss_week = int(line[0])
            week_seconds = float(line[1])
            time = GNSS_START_TIME + timedelta(weeks=gnss_week, seconds=week_seconds)
            return time

if __name__ == "__main__":
    from pathlib import Path
    nov_file = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/NCL1/HR/329653_001_20230629_064149_00285_NOV770.raw")
    out = nov_file.parent / "output_nov.log"
    logid = "NOV"
    ts = get_date(nov_file)
    print(ts)
    # parser = argparse.ArgumentParser(prog=sys.argv[0])
    # parser.add_argument(
    #     "-o",
    #     "--output",
    #     nargs="?",
    #     help="Output file name (default: output.log)",
    #     default="output.bin",
    # )
    # parser.add_argument(
    #     "-i",
    #     "--input",
    #     nargs="?",
    #     help="Path to input file (default: ./* (all files in current directory))",
    #     default="./*",
    # )
    # parser.add_argument(
    #     "-l",
    #     "--logid",
    #     nargs="?",
    #     help="Log file ID, i.e. NOV, GNSS (default: None (all .bin files))",
    #     default="None",
    # )

    # args = parser.parse_args()

    # main(args.output, args.input, args.logid)
    # logging.info("Done!")
