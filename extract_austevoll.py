import pandas as pd
import numpy as np
import argparse
import xml.etree.ElementTree as ETree
from dateutil import parser
import os
import logging
import time
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

"""
Use this python routine to create a python dataframe as a feather file. The feather file can be read in 
as a dataframe a further processed with pandas.read_feather(filename).
"""
class ExtractAanderaaData():
    def __init__(self, directory_path: str, data_path: str, dataset_id: str, scan_first: bool = True):
        """

        :param directory_path: path where the device data is available (represented as XML files)
        :param data_path: path for the folder to write the resulting dataset
        :param dataset_id: name to use for the dataset that is created (this becomes part of the filename)
        :param scan_first: set to false if the entire directory should not be processed on creation
        """
        self.directory_path = directory_path
        self.dataset_id = dataset_id
        self.data_path = data_path

        logger.info(f"parsing sensor data in {self.directory_path}")
        if scan_first:
            self.process_data_directory(self.directory_path)
            self.process_data_create_feather()

    namespaces = {"doc": "http://www.aadi.no/RTOutSchema"}

    times = []
    longs = []
    lats = []
    instr_data_dict = {'Time': [], 'Long': [], 'Lat': []}
    prof_dictionaries = {}

    @staticmethod
    def is_float(element) -> bool:
        try:
            float(element)
            return True
        except ValueError:
            return False

    @staticmethod
    def make_instrument_name(attrib: {}) -> str:
        if 'ProdName' in attrib:
            name = attrib['ProdName']
        else:
            name = attrib['Descr']

        return name.strip().replace(" ", "_")

    @staticmethod
    def make_name(descr):
        my_descr = descr
        my_descr = my_descr.strip().replace(" ", "_")
        return my_descr

    def extract_prof_data(self, sensor_data, long, lat, from_time):

        instr_name = self.make_instrument_name(sensor_data.attrib)
        if instr_name not in self.prof_dictionaries:
            self.prof_dictionaries[instr_name] = {'Time': [], 'Long': [], 'Lat': [], 'Depth': []}
            logger.info("New profiler instrument {}".format(instr_name))

        prof_data_dict = self.prof_dictionaries[instr_name]

        for profile in sensor_data.findall('.//doc:Profile', self.namespaces):
            # print("profile " + profile.attrib['ID'])
            for column in sensor_data.findall('.//doc:Column', self.namespaces):

                cell_size = int(column.attrib['CellSize'])
                column_start = 0
                if 'ColumnStart' in column.attrib:
                    column_start = float(column.attrib['ColumnStart']) + cell_size/2.0
                elif 'ColumnStartCellCenter' in column.attrib:
                    column_start = float(column.attrib['ColumnStartCellCenter'])

                dist_between_cells = 0
                if 'CellOverlap' in column.attrib:
                    cell_overlap = float(column.attrib['CellOverlap']) / 100
                    dist_between_cells = (1 - cell_overlap) * cell_size

                if 'CellCenterSpacing' in column.attrib:
                    dist_between_cells = float(column.attrib['CellCenterSpacing'])

                prof_point_name_dict = {}
                cell_attributes = column.find('.//doc:CellAttributes', self.namespaces)
                for point in cell_attributes.findall('doc:Point', self.namespaces):
                    point_name = self.make_name(point.attrib['Descr'])
                    prof_point_name_dict[point.attrib['ID']] = point_name
                    if point_name not in prof_data_dict:
                        num_rows = len(prof_data_dict['Time'])
                        if num_rows > 0:
                            prof_data_dict[point_name] = [np.nan] * num_rows
                        else:
                            prof_data_dict[point_name] = []

                for cell in column.findall('.//doc:Cell', self.namespaces):
                    index = int(cell.attrib['Index'])
                    depth = column_start + index * dist_between_cells
                    prof_data_dict['Time'].append(from_time)
                    prof_data_dict['Long'].append(float(long))
                    prof_data_dict['Lat'].append(float(lat))
                    prof_data_dict['Depth'].append(float(depth))
                    for point in cell.findall('doc:Point', self.namespaces):
                        point_id = point.attrib['ID']
                        val = point.find('doc:Value', self.namespaces).text
                        if val is not None:
                            if self.is_float(val):
                                prof_data_dict[prof_point_name_dict[point_id]].append(float(val))
                            else:
                                prof_data_dict[prof_point_name_dict[point_id]].append(val)
                        else:
                            prof_data_dict[prof_point_name_dict[point_id]].append(np.nan)

                    num_times = len(prof_data_dict['Time'])
                    for key, values in prof_data_dict.items():
                        l0 = len(values)
                        if l0 < num_times:
                            logger.info("Missing value for {}, appending {} nan's".format(key, (num_times - l0)))
                            additions = [np.nan] * (num_times - l0)
                            prof_data_dict[key].extend(additions)

    def extract_instrument_data(self, sensor_data):
        # logger.info(f'extract_instrument_data processing sensor_data')
        instr_name = self.make_instrument_name(sensor_data.attrib)

        parameters = sensor_data.find('.//doc:Parameters', self.namespaces)
        for point in parameters.findall('doc:Point', self.namespaces):
            point_name = instr_name + '.' + self.make_name(point.attrib['Descr'])
            if point_name not in self.instr_data_dict:
                num_rows = len(self.instr_data_dict['Time'])
                if num_rows > 1:
                    self.instr_data_dict[point_name] = [np.nan] * (num_rows-1)
                else:
                    self.instr_data_dict[point_name] = []

            val = point.find('doc:Value', self.namespaces).text
            if val is not None:
                if self.is_float(val):
                    self.instr_data_dict[point_name].append(float(val))
                else:
                    self.instr_data_dict[point_name].append(val)
            else:
                self.instr_data_dict[point_name].append(np.nan)

    def _load_sensor_data(self, file_path_in):
        # logger.info(f'_load_sensor_data processing file {file_path_in}')
        loaded = False
        load_tries = 0
        while (not loaded) and (load_tries < 10):
            # Try a few times then give up
            load_tries += 1
            try:
                doc = ETree.parse(file_path_in)
                loaded = True
            except ETree.ParseError:
                time.sleep(1)
            except Exception:
                raise

        if not loaded:
            logger.warning(f'Cannot parse {file_path_in}')
            return

        root = doc.getroot()
        data = root.find('doc:Data', self.namespaces)

        long = lat = 0

        if len(self.instr_data_dict['Long']) > 0:
            long = self.instr_data_dict['Long'][-1]
            lat = self.instr_data_dict['Lat'][-1]

        for system_info in root.findall('*//doc:SystemInfo', self.namespaces):
            # print(system_info.attrib['Descr'])
            try:
                if system_info.attrib['Descr'] == 'GeoPosition':
                    pos_str = system_info.text.split(',')
                    long = float(pos_str[1])
                    lat = float(pos_str[0])
            except: # In the case that GeoPosition is available but no value set (GeoPosition is optional)
                long = -999
                lat = -90

        for data in root.findall('doc:Data', self.namespaces):
            time_str = data.find('doc:Time', self.namespaces).text
            from_time = parser.isoparse(time_str)
            self.instr_data_dict['Long'].append(long)
            self.instr_data_dict['Lat'].append(lat)
            self.instr_data_dict['Time'].append(from_time)
            num_times = len(self.instr_data_dict['Time'])
            for sensorData in data.findall('doc:SensorData', self.namespaces):
                self.extract_instrument_data(sensorData)
                if sensorData.find('*//doc:Profile', self.namespaces):
                    self.extract_prof_data(sensorData, long, lat, from_time)
            for systemData in data.findall('doc:SystemData', self.namespaces):
                self.extract_instrument_data(systemData)

            for key, values in self.instr_data_dict.items():
                l0 = len(values)
                if l0 < num_times:
                    logger.info("Missing value for {}, appending {} nan's".format(key, (num_times - l0)))
                    additions = [np.nan] * (num_times - l0)
                    self.instr_data_dict[key].extend(additions)

    def process_data_directory(self, directory):
        logger.info(f'process_data_directory processing folder {directory}')
        for file in os.listdir(directory):
            filename = os.fsdecode(os.path.join(directory, file))
            self.process_data_onefile(filename)
        return

    def process_data_onefile(self, file):
        logger.debug(f'process_data_directory processing file {file}')
        filename = os.fsdecode(file)
        if filename.endswith(".xml"):
            self._load_sensor_data(filename)
        return

    def process_data_create_feather(self):
        for key, value in self.prof_dictionaries.items():
            prof_dict = value
            prof_df = pd.DataFrame.from_dict(value, orient='columns').sort_values(by='Time')
            prof_file = os.path.join(os.path.abspath(self.data_path), self.dataset_id + '_' + key) + ".feather"
            prof_df.reset_index(drop=True).to_feather(prof_file)

        instrument_df = pd.DataFrame.from_dict(self.instr_data_dict, orient='columns').sort_values(by='Time')
        instrument_file = os.path.join(os.path.abspath(self.data_path), self.dataset_id + '_instrument') + ".feather"
        instrument_df.reset_index(drop=True).to_feather(instrument_file)

if __name__ == "__main__":
    logger.setLevel(20)

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--directory_path", help="Path to folder where source files are stored", type=str, default=None)
    arg_parser.add_argument("--dataset_id", help="Filename of the dataset written", type=str, default='Austevoll_data')
    arg_parser.add_argument("--data_path", help="Path to store the resulting dataset", type=str, default='.')
    args = arg_parser.parse_args()

    ExtractAanderaaData(args.directory_path, data_path=args.data_path, dataset_id=args.dataset_id)
