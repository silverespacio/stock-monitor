# data_reader.py
import pandas as pd


class Reader:
    def __init__(self, file_name):
        self.file_name = file_name
        self.data = pd.DataFrame()
        self.read_file()
        self.process_data()

    def read_file(self):
        self.data = pd.read_csv('C:/Trabajos/Transacciones-Tarjetas/files/' + self.file_name,
                                sep=';',
                                header='infer',
                                encoding='iso-8859-1')
    def process_data(self):
        columns_tmp = self.data.columns
        columns_tmp = [column_name.lower() for column_name in columns_tmp]
        self.data.columns = columns_tmp
        
if __name__ == '__main__':
    pass