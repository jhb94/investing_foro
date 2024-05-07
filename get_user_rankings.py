import requests
from bs4 import BeautifulSoup
import pandas as pd
import json

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

'''
SAMPLE PAYLOAD:
<tr>
    <td class="first left">1</td>
    <td class="left">Marcos Rolf Fischer Stauber</td>
    <td>86</td>
    <td>86</td>
    <td>78</td>
    <td class="right">90.7</td>
    <td class="bold right greenFont">+122.12%</td>
</tr>
<tr>
    <td class="first left">2</td>
    <td class="left">Francisco Garcia</td>
    <td>1</td>
    <td>1</td>
    <td>1</td>
    <td class="right">100</td>
    <td class="bold right greenFont">+47.11%</td>
</tr>
'''


## Company Sentiments url.
sentiments_url = f'https://es.investing.com/instruments/sentiments/recentsentimentsAjax?action=get_sentiments_rows&pair_decimal_change=4&pair_decimal_change_percent=2&pair_decimal_last=4&sentiments_category=pairs&pair_ID='

## Below value is the separator that investing uses when the user inputs both end date and a predicted exact value
end_date_prediction_separator = ' @ '

'''
SAMPLE PAYLOAD:
        {
            "start_date": "03.05.2024",
            "site_id": "4",
            "user_id": "255840213",
            "username": "Rafael Navarro",
            "iconType": "Bear",
            "open": "0.1840",
            "end_date": "06.05.2024 @ 0.1800",
            "font_color": "greenFont",
            "var_tmp_plus": "+",
            "change_percent": "2.17"
        }
'''

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
    
    ## Truncate user name to 19 characters as it appears in sentiments page, for later comparisson with that dataframe.
    ## This is a VLOOKUP by user name
    rankings_list['Usuario'] = rankings_list['Usuario'].str.slice(0,19)

    return rankings_list

def apply_trust_conditions(rankings_list,  win_percentage, number_of_predictions, variation_percentage):

    adjusted_rankings = rankings_list[rankings_list['Gan. %'] > win_percentage]
    adjusted_rankings = adjusted_rankings[adjusted_rankings['Total'] > number_of_predictions]
    adjusted_rankings = adjusted_rankings[adjusted_rankings['% Var.'] > variation_percentage]

    return adjusted_rankings

def find_latest_user_prediction(user_name):

    iterator = 0

    ## Read Data.
    recent_sentiments = requests.get(f'{sentiments_url}{identifier}&sentimentsBulkCount={iterator}')

    recent_sentiments_json = json.loads(recent_sentiments.content.decode() )

    recent_sentiments_list = pd.DataFrame.from_dict(recent_sentiments_json['rowsData'] )

    ## Parse and convert date to real pandas datetime.
    recent_sentiments_list['start_date'] = pd.to_datetime(recent_sentiments_list['start_date'], format="%d.%m.%Y", dayfirst=True)

    user_sentiments_df = recent_sentiments_list[recent_sentiments_list["username"].str.contains(user_name)]

    return user_sentiments_df[user_sentiments_df['start_date'] == user_sentiments_df['start_date'].max()]


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

    print('-----------------------')

    print('Looking for trustable users latest predictions')

    for index, trusted_user in trusted_users.iterrows():

        print('Current user: ', trusted_user['Usuario'], '\n Looking for user latest prediction...')

        last_user_prediction = find_latest_user_prediction(trusted_user['Usuario'])

        if not last_user_prediction.empty:

            print('Last prediction of user is: ')
            print('-----------------------')
            print(last_user_prediction)
        
        else:
            
            print('-----------------------')
            print('This user does not have any recent predictions')

if __name__ == '__main__' :
    main()
    
