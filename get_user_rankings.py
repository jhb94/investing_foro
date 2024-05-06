import requests
import os
import sys
from bs4 import BeautifulSoup
import pandas as pd

ezentis_identifier = '32237'

rankings_url = f'https://investing.com/common/sentiments/sentiments_ajax.php?action=get_user_rankings_bulk_records&item_ID={ezentis_identifier}&sentimentsBulkCount='

def get_records():

    iterator = 0
    response = ''
    records_left = True

    while records_left:
        
        rankings_bulk_records = requests.get(f'{rankings_url}{iterator}')

        if rankings_bulk_records.content == b'':
            records_left = False
        else:
            response += rankings_bulk_records.content.decode()
            iterator += 1
    
    return response

def main (iterator: int):

        header_data = [['Rango', 'Usuario', 'Total',	'Cerrados',	'Ganadores','Gan. %','% Var.']]

        records = get_records()

        table_data = [[cell.text for cell in row("td")]
                        for row in BeautifulSoup(records, "html.parser")("tr")]

        rankings = header_data + table_data

        rankings_list = pd.DataFrame(rankings[1:], columns=rankings[0])

        print(rankings_list)

        iterator += 1

if __name__ == '__main__' :
    main(1)
    
