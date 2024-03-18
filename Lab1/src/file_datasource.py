from csv import reader
from datetime import datetime
from domain.aggregated_data import AggregatedData
from domain.accelerometer import Accelerometer
from domain.parking import Parking
from domain.gps import Gps

class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.accelerometer_data = []
        self.gps_data = []
        self.parking_filename = parking_filename
        self.parking_data = []

    def startReading(self):
    # Reading accelerometer data
        with open(self.accelerometer_filename, 'r') as file:
            csv_reader = reader(file)
            next(csv_reader, None)  # Skip the header
            for row in csv_reader:
                if len(row) >= 3:  # Check if row has at least 3 elements
                    try:
                        x = int(row[0])
                        y = int(row[1])
                        z = int(row[2])
                        self.accelerometer_data.append(Accelerometer(x=x, y=y, z=z))
                    except ValueError as e:
                        print(f"Error processing accelerometer data row: {row} - {e}")
                else:
                    print(f"Row does not have enough elements: {row}")

    # Reading GPS data
        with open(self.gps_filename, 'r') as file:
            csv_reader = reader(file)
            next(csv_reader, None)  # Skip the header
            for row in csv_reader:
                if len(row) >= 2:  # Check if row has at least 2 elements
                    try:
                        longitude = float(row[0])
                        latitude = float(row[1])
                        self.gps_data.append(Gps(longitude=longitude, latitude=latitude))
                    except ValueError as e:
                        print(f"Error processing GPS data row: {row} - {e}")
                else:
                    print(f"Row does not have enough elements: {row}")
    
    def startReadingParking(self):
        with open(self.parking_filename, 'r') as file:
            csv_reader = reader(file)
            next(csv_reader, None)  # Skip the header
            for row in csv_reader:
                if len(row) >= 3: 
                    try:
                        empty_count = int(row[0])
                        # Assuming the GPS data is in columns 1 and 2
                        gps = Gps(longitude=float(row[1]), latitude=float(row[2]))
                        self.parking_data.append(Parking(empty_count=empty_count, gps=gps))
                    except ValueError as e:
                        print(f"Error processing parking data row: {row} - {e}")


    def read(self) -> AggregatedData:
        if not self.accelerometer_data and not self.gps_data and not self.parking_data:
            return AggregatedData(accelerometer=None, gps=None, parking=None, time=datetime.now())

        parking = self.parking_data.pop(0) if self.parking_data else Parking(empty_count=0, gps=Gps(longitude=0.0, latitude=0.0))
        accelerometer = self.accelerometer_data.pop(0) if self.accelerometer_data else None
        gps = self.gps_data.pop(0) if self.gps_data else None

        return AggregatedData(accelerometer=accelerometer, gps=gps, parking=parking, time=datetime.now())

    def stopReading(self):
        self.accelerometer_data = []
        self.gps_data = []

