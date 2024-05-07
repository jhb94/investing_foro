import requests
from bs4 import BeautifulSoup
import pandas as pd

## identifier should be a console configurable input parameter
identifier = '32237' # EZENTIS: 32237

## Rankings url, in the form of https://investing.com/common/sentiments/sentiments_ajax.php?action=get_user_rankings_bulk_records&item_ID=32237&sentimentsBulkCount=0
## Where sentimentsBulkCount is a paginator that groups 50 users per page.
## Fixed param.
rankings_url = f'https://investing.com/common/sentiments/sentiments_ajax.php?action=get_user_rankings_bulk_records&item_ID='

## Conditions we need to apply to a user in order to trust the predictions made.
win_percentage = 80
number_of_predictions = 10
variation_percentage = 10

## Function that receives the identifier or symbol of a company and returns all the users that made predictions
def get_user_ranking(identifier):

    iterator = 0
    response = ''
    records_left = True
    header_data = [['Rango', 'Usuario', 'Total',	'Cerrados',	'Ganadores','Gan. %','% Var.']]

    while records_left:
        
        rankings_bulk_records = requests.get(f'{rankings_url}{identifier}&sentimentsBulkCount={iterator}')

        if rankings_bulk_records.content == b'':
            records_left = False
        else:
            response += rankings_bulk_records.content.decode()
            iterator += 1
    
    table_data = [[cell.text for cell in row("td")]
                        for row in BeautifulSoup(response, "html.parser")("tr")]

    rankings = header_data + table_data

    rankings_list = pd.DataFrame(rankings[1:], columns=rankings[0])

    rankings_list = rankings_list.astype({'Total': 'int32', 'Cerrados': 'int32', 'Ganadores': 'int32'})

    rankings_list['% Var.'] = rankings_list['% Var.'].str.replace('%', '', regex=False).astype(float)
    rankings_list['Gan. %'] = rankings_list['Gan. %'].astype(float)

    return rankings_list

def apply_trust_conditions(rankings_list,  win_percentage, number_of_predictions, variation_percentage):

    adjusted_rankings = rankings_list[rankings_list['Gan. %'] > win_percentage]
    adjusted_rankings = adjusted_rankings[adjusted_rankings['Total'] > number_of_predictions]
    adjusted_rankings = adjusted_rankings[adjusted_rankings['% Var.'] > variation_percentage]

    return adjusted_rankings
def main ():

    rankings_list = get_user_ranking(identifier)

    trusted_users = apply_trust_conditions(rankings_list,  win_percentage, number_of_predictions, variation_percentage)

    print('-----------------------')

    print('ADJUSTED RANKING OF USERS THAT MADE PREDICTIONS ON SYMBOL :', identifier)
    print('PARAMETERS:')
    print('WON % RATE :  ', win_percentage)
    print('TOTAL NUMBER OF PREDICTIONS : ', number_of_predictions)
    print('SUBYACENT % VARIATION RATE : ', win_percentage)

    print('-----------------------')

    print(trusted_users)

    print('Total of users : ', len(trusted_users) )

if __name__ == '__main__' :
    main()
    
