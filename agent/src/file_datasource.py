from csv import reader, DictReader
from marshmallow import Schema
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.aggregated_data import AggregatedData
from schema.accelerometer_schema import AccelerometerSchema
from schema.gps_schema import GpsSchema
import config
from enum import Enum


class FileDatasource:
    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
    ) -> None:
        self.readers = dict()
        self.readers[DataType.ACCELEROMETER] = CSVDatasourceReader(
            accelerometer_filename, AccelerometerSchema()
        )
        self.readers[DataType.GPS] = CSVDatasourceReader(
            gps_filename, GpsSchema()
        )

    def read(self) -> AggregatedData:
        """Метод повертає дані отримані з датчиків"""
        try:
            accelerometer = self.readers[DataType.ACCELEROMETER].read()
            gps = self.readers[DataType.GPS].read()

            timestamp = datetime.now()

            return AggregatedData(accelerometer, gps, timestamp)
        except Exception as e:
            print(f"Error while reading data from sensors: {e}")

    def startReading(self, *args, **kwargs):
        """Метод повинен викликатись перед початком читання даних"""
        for reader in self.readers.values():
            reader.startReading()

    def stopReading(self, *args, **kwargs):
        """Метод повинен викликатись для закінчення читання даних"""
        for reader in self.readers.values():
            reader.stopReading()

class DataType(Enum):
    ACCELEROMETER = 1
    GPS = 2

class CSVDatasourceReader:
    filename: str
    reader: DictReader

    def __init__(self, filename, schema: Schema):
        self.filename = filename
        self.schema = schema

    def startReading(self):
        self.file = open(self.filename, 'r')
        self.reader = DictReader(self.file)

    def read(self):
        row = next(self.reader, None)

        if row is None:
            self.reset()
            row = next(self.reader, None)

        return self.schema.load(row)

    def reset(self):
        self.file.seek(0)
        self.reader = DictReader(self.file)

    def stopReading(self):
        if self.file:
            self.file.close()
