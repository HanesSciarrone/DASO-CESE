import signal
import sys
import socket
import time
import csv
import json

class DataCSV:
    def __init__(self, path):
        self.set_path(path)

    def set_path(self, path):
        self.__path = path

    def get_path(self):
        return self.__path 

    def get_data_CSV(self):
        list = []
        with open(self.get_path()) as csv_fd:
            csv_reader = csv.reader(csv_fd)
            next(csv_reader)
            for row in csv_reader:
                element = { "id": int(row[0]), "name": str(row[1]), "value1": float(row[2]), "value2": float(row[3])}
                print(element)
                list.append(element)
            print(list)
        return json.dumps(list)

    

class Main:
    def __init__(self, name):
        self.set_file_name(name)

    def set_file_name(self, name):
        self.__path = name

    def get_file_name(self):
        return self.__path

    def get_path_CSV(self):
        with open(self.get_file_name(), 'r', encoding='utf-8') as fd:
            return fd.read()

    # Function that hand signal
    def handler_signal(self, sig, frame):
        print('I receive señal SIGINT')
        exit(0)

    def main(self):

        #
        port = 10000
        
        # Handler of signal
        signal.signal(signal.SIGINT, self.handler_signal)   

        # Set port where will sent data
        try:
            port = int(sys.argv[1])
            print('Port of connection is %d'% port)
        except:
            print('Incorrect port')
            exit(1)

        # Create object DATACSV
        data_csv = DataCSV(self.get_path_CSV())
        
        while True:
            try:
                # Get JSON from CSV file
                string_json = data_csv.get_data_CSV()
                print(string_json)

                # Create socket and send data to PizarraService.py
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(bytearray(string_json, 'utf-8'), ('localhost', port))

                # sleep during 30 second
                time.sleep(30)  
            finally:
                print('Close socket')
                sock.close()
        

        
m = Main('Config.txt')
m.main()
    


