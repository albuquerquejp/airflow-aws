from datetime import datetime
import os

def process_file(**kwargs):
    json_output = []
    with open('sample.txt') as sourcefile:
        for line in sourcefile:
            record = {}
            for key, value in kwargs.items():
                start, end = value
                record[key] = line[start:end].strip()
            json_output.append(record)

    print(json_output)

if __name__ == "__main__":
    
    kwargs = {
        'id': (0, 5),
        'Nome': (5, 20),
        'telefone': (20, 30),
        'DtNascimento': (30, 40),
        'Endereço': (40, 50)
    }

    process_file(**kwargs)

    print('-'*100)

    process_file(id=(0, 5),
        Nome=(5, 20),
        telefone= (20, 30),
        DtNascimento=(30, 40),
        Endereço=(40, 50))