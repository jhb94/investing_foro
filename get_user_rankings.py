import requests
import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import json
import boto3
import calendar
from datetime import datetime
from typing import List
import time
import random
import utils.proxy_page_port_functionality as proxy_class

scraper = cloudscraper.create_scraper()

# Initialize the BOTO3 client
s3 = boto3.resource('s3', region_name="eu-west-1")

# Initialize the SES client
ses_client = boto3.client("ses", region_name="eu-west-1")

## Rankings url, in the form of https://investing.com/common/sentiments/sentiments_ajax.php?action=get_user_rankings_bulk_records&item_ID=32237&sentimentsBulkCount=0
## Where sentimentsBulkCount is a paginator that groups 50 users per page.
rankings_url = 'es.investing.com/common/sentiments/sentiments_ajax.php?action=get_user_rankings_bulk_records&item_ID='

'''
SAMPLE PAYLOAD: 
<tr>
    <td class="first left">1</td>
    <td class="left">Marcos Rolf Fischer Stauber</td>
        <a href="/members/200547541/sentiments-equities">Marcos Rolf Fischer Stauber</a>
    <td>86</td>
    <td>86</td>
    <td>78</td>
    <td class="right">90.7</td>
    <td class="bold right greenFont">+122.12%</td>
</tr>
<tr>
    <td class="first left">2</td>
    <td class="left">Francisco Garcia</td>
        <a href="/members/200547998/sentiments-equities">Francisco Garcia</a>
    <td>1</td>
    <td>1</td>
    <td>1</td>
    <td class="right">100</td>
    <td class="bold right greenFont">+47.11%</td>
</tr>
'''


## Company Sentiments url - NOT IN USE
# sentiments_url = f'https://es.investing.com/instruments/sentiments/recentsentimentsAjax?action=get_sentiments_rows&pair_decimal_change=4&pair_decimal_change_percent=2&pair_decimal_last=4&sentiments_category=pairs&pair_ID='

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
## IMPORTANT: As the user link will only be displayed on the page belonging to the user's preferred language, it is neccessary
## To loop all the countries and form a combined dataframe with all the data. Then rows whoose UserLink is null are dropped.
def get_user_ranking(identifier:str, countries: List[str]):

    # rankings_list_full = None

    # for country in countries:

    iterator = 0
    response = ''
    records_left = True
    header_data = [['Rango', 'Usuario', 'Total',	'Cerrados',	'Ganadores','Gan. %','% Var.', 'UserLink']]

    while records_left:

        # rankings_bulk_records = requests.get(f'https://{country}{rankings_url}{identifier}&sentimentsBulkCount={iterator}')
        rankings_bulk_records = requests.get(f'https://{rankings_url}{identifier}&sentimentsBulkCount={iterator}')
        
        ## 403 forbidden
        ## 429 Rate limited.
        if rankings_bulk_records.status_code != 200:
            break

        if rankings_bulk_records.content == b'':
            records_left = False
        else:
            response += rankings_bulk_records.content.decode()
            iterator += 1
    
    if len(response) == 0:
        next 
    
    soup = BeautifulSoup(response, "html.parser")

    table_data = [[cell.text for cell in row("td")]
                        for row in soup("tr")]

    # Extracting links from 'a' tags, this links point to user pages. Links are in the second td tag inside the href of the link
    links = [row.find_all("td")[1].find("a")["href"] if row.find_all("td")[1].find("a") else None for row in soup("tr")]

    rows = [ row + [link] for row, link in zip(table_data, links)]

    rankings = header_data + rows

    rankings_list = pd.DataFrame(rankings[1:], columns=rankings[0])

    rankings_list = rankings_list.astype({'Total': 'int32', 'Cerrados': 'int32', 'Ganadores': 'int32'})

    rankings_list['% Var.'] = rankings_list['% Var.'].str.replace('%', '', regex=False).astype(float)
    rankings_list['Gan. %'] = rankings_list['Gan. %'].astype(float)
    rankings_list['UserLink'] =  rankings_list['UserLink'].str.replace('currencies', 'equities')

    # rankings_list = rankings_list.dropna(subset=['UserLink'])

    # if  rankings_list_full is None:
    #     rankings_list_full = rankings_list
    # else:
    #     rankings_list_full = pd.concat([rankings_list_full , rankings_list], axis=0)

    return rankings_list


def apply_trust_conditions(rankings_list : pd.DataFrame,  win_percentage : float, number_of_predictions: int, variation_percentage: float):

    adjusted_rankings = rankings_list[rankings_list['Gan. %'] >= win_percentage]
    adjusted_rankings = adjusted_rankings[adjusted_rankings['Total'] >= number_of_predictions]
    adjusted_rankings = adjusted_rankings[adjusted_rankings['% Var.'] >= variation_percentage]

    return adjusted_rankings

def find_latest_user_prediction_scrapper(user_link: str, company_name:str, proxies: List[str]):

    ## TODO: Replace AWS Proxy solution as It may stop working at some point and is dangerous
    ## Taken from Irish proxy list: https://spys.one/free-proxy-list/IE/

    retries = 2

    while retries > 0:
        try:

            random_proxy = random.choice(proxies)
            proxy_type = random_proxy.split(":")[0].lower()
            proxy = {proxy_type: random_proxy}

            user_equities_sentiments_html_page = scraper.get(f'https://es.investing.com{user_link}', timeout=10, proxies=proxy)
            
            ## 403 too many requests
            ## 504 user page does not load
            if user_equities_sentiments_html_page.status_code != 200:
                print(f"Page {user_link} couldn't be loaded, status code: {user_equities_sentiments_html_page.status_code}")
                
                if user_equities_sentiments_html_page.status_code == 429 :
                    
                    print("Rate Limit (429) exceeded, waiting 20s to retry....")
                    time.sleep(20)
                
                if user_equities_sentiments_html_page.status_code == 403 :
                    
                    print("Getting FORBIDDEN (403) status responses, waiting 20s before retrying....")
                    time.sleep(20)

            retries -= 1

        except Exception as e:
            print(f"Error processing GET request to 'https://es.investing.com{user_link}'. ERROR: {e}")
            

    ## TODO: Accept only 200
    soup = BeautifulSoup(user_equities_sentiments_html_page.text, "html.parser")

    sentiments_table = soup.find('table', attrs={'id':'sentiments_table'})
    
    if sentiments_table is None:
        print(f"User data from link {user_link} could not be retrieved, sentiments object is NULL")
        return None

    table_data = [[cell.text for cell in row("td")]
                        for row in sentiments_table.find_all('tr')]

    table_data = table_data[1:]

    header_data = [['PredictionDate', 'Name', 'direction',   'Open' , 'Forecast' , '% Var.']]

    user_sentiments = header_data + table_data

    user_sentiments_list = pd.DataFrame(user_sentiments[1:], columns=user_sentiments[0])
    
    user_sentiments_list['% Var.'] = user_sentiments_list['% Var.'].str.replace('%', '', regex=False).astype(float)

    ## Remove all the sentiments that do not have a specific forecast.
    user_sentiments_list = user_sentiments_list[user_sentiments_list['Forecast'].str.contains('@')]

    ## Convert Forecast column into 2 columns with expected value and date for the value:
    user_sentiments_list[['DateForTheValue', 'PredictionValue']] = user_sentiments_list['Forecast'].str.split(' @ ', expand=True)
    
    user_sentiments_list = user_sentiments_list[user_sentiments_list['Name'] == company_name]

    # Drop the original 'Forecast' column
    user_sentiments_list = user_sentiments_list.drop(columns=['Forecast'])

    ## Parse both Dates
    user_sentiments_list['PredictionDate'] = pd.to_datetime(user_sentiments_list['PredictionDate'].str[:-2] + '20' + user_sentiments_list['PredictionDate'].str[-2:], format="%d-%m-%Y", dayfirst=True)
    user_sentiments_list['DateForTheValue'] = pd.to_datetime(user_sentiments_list['DateForTheValue'].str[:-2] + '20' + user_sentiments_list['DateForTheValue'].str[-2:], format="%d-%m-%Y", dayfirst=True)

    user_sentiments_list = user_sentiments_list[user_sentiments_list['PredictionDate'] == user_sentiments_list['PredictionDate'].max()]

    user_sentiments_list = user_sentiments_list.drop("direction", axis =1 )

    ## Drop duplicates based on start date colum (it is not a timestamp so not able to get last based on hours)
    ## Keep last value of the prediction 
    return user_sentiments_list.drop_duplicates(subset=['PredictionDate'], keep='first')

## Send email to recipients with new predictions
def send_email(last_user_prediction: pd.DataFrame, prediction_notification_email_from: str, prediction_notification_email_to: List[str] ): 

    CHARSET = "UTF-8"

    prediction_notification_email_text = "NEW USER PREDICTION PUBLISHED: \n" 
    prediction_notification_email_title="INVESTING.COM Alerting system on: {Day}/{Month}/{Year}"

    body = """\
        <html>
        <head></head>
        <body>
            {0}
        </body>
        </html>
        """.format(last_user_prediction.to_html())

    # Send the email
    response = ses_client.send_email(
        Source=prediction_notification_email_from,
        Destination={
            'ToAddresses': [
                address for address  in prediction_notification_email_to
            ],
        },
        Message={
            'Body': {
                'Html': {
                    'Charset': CHARSET,
                    'Data': body,
                },
                'Text': {
                    'Charset': CHARSET,
                    'Data': prediction_notification_email_text,
                },
            },
            'Subject': {
                'Charset': CHARSET,
                'Data':  prediction_notification_email_title.format(
                                Year=datetime.now().year,
                                Month=calendar.month_name[datetime.now().month],
                                Day=datetime.now().day,
                            ),
            },
        },
    )

    if (
        "ResponseMetadata" in response
        and "HTTPStatusCode" in response["ResponseMetadata"]
        and response["ResponseMetadata"]["HTTPStatusCode"] == 200
    ):
        print("Email sent successfully.")
    else:
        print(  # pylint: disable=logging-fstring-interpolation
            f"Failed to send Email. Status Code: {response.status_code}, Response: {response.text}"
        )

def main ():
    
    ## Get some proxies in order to make the calls for us since AWS EC2 and service instances IP's are generally banned in cloudfare.
    # proxies_url = 'https://spys.one/free-proxy-list/US/'
    proxies_url = 'https://spys.one/free-proxy-list/ES'

    proxies = proxy_class.get_proxies(proxies_url)

    ## READ INITIALIZATION FILES ##
    ## ------------------------- ##

    companies_file  = 'companies_to_watch.json'
    previous_sentiments_file = 'latest_reliable_sentiments.json'

    bucket_name = 'investing.com-predictions-project-bucket'

    content_object_companies = s3.Object(bucket_name, companies_file)
    file_content_companies = content_object_companies.get()['Body'].read().decode('utf-8')

    companies_to_watch =  json.loads(file_content_companies)

    content_object_sentiments = s3.Object(bucket_name, previous_sentiments_file)
    file_content_sentiments = content_object_sentiments.get()['Body'].read().decode('utf-8')

    previous_sentiments =  json.loads(file_content_sentiments)
    
    reliable_sentiments_json =  pd.DataFrame(previous_sentiments["reliable_sentiments"])
    
    with open('config.json', 'r') as config_file:
        app_config = json.load(config_file)

    ## ------------------------- ##

    ## READ USER DATABASE CSV CONTAINING USER ID's ##
    ## ------------------------- ##
    ## May below be non-performant and could be better to use an indexed sqlite db instead for faster access
    ## How-to in this link: https://www.sqlitetutorial.net/sqlite-import-csv/

    # UNCOMMENT if user database in place
    # user_database = pd.read_csv('user_data.csv', header=True)

    ## ------------------------- ##


    for i in companies_to_watch["companies"]:

        identifier = i["identifier"]
        company_name = i["name"]
        win_percentage = i["win_percentage"]
        number_of_predictions = i["number_of_predictions"]
        variation_percentage = i["variation_percentage"]

        print("Updating data predictions for company: ", company_name , " with identifier: ", identifier)

        rankings_list = get_user_ranking(identifier, app_config["countries"])

        print(rankings_list)

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

        for _, trusted_user in trusted_users.iterrows():

            print('Current user: ', trusted_user['Usuario'], '\n Looking for user latest prediction...')

            if trusted_user['UserLink'] is not None and trusted_user['UserLink'] != '' :

                last_user_prediction = find_latest_user_prediction_scrapper(trusted_user['UserLink'], company_name, proxies)

                if not last_user_prediction:
                    next

            elif trusted_user['UserLink'] == '':
                
                ## Format: members/200303883/sentiments-equities

                ## UNCOMMENT if user database in place
                # user_link = user_database[user_database['user_name'] == trusted_user['Usuario']]['user_id']
                # trusted_user['UserLink'] = f'/members/{user_link}/sentiments-equities'

                print(f"User {trusted_user['Usuario']} meets the requirements but has no link in this domain")

            else:
                next

            if  last_user_prediction is not None:
                
                last_user_prediction['UserName'] = trusted_user['Usuario'] + trusted_user['UserLink'].replace('/members/', '(').replace('/sentiments-equities', ')')

                print('Last prediction of user is: ')
                print('-----------------------')
                print(last_user_prediction)

                # Check if the new row exists in the JSON data
                reliable_sentiments_json_already_in_list = pd.concat([reliable_sentiments_json.astype(str), last_user_prediction.astype(str)], ignore_index=True)

                if not reliable_sentiments_json_already_in_list.duplicated().isin([True]).any():
                    
                    print("Sending information via email....")
                    send_email(last_user_prediction, app_config["emailFrom"] , app_config["emailTo"] )

                    print("EMAIL SENT")

                    print('Adding new sentiment ENTRY to the list')
                    reliable_sentiments_json = pd.concat([reliable_sentiments_json, last_user_prediction], ignore_index=True)

                else : 
                    print("Entry already exist in predictions JSON list")
            
            else:
                
                print('-----------------------')
                print('This user does not have any recent predictions')

    print('Updating json file')

    reliable_sentiments_json = reliable_sentiments_json.astype(str)

    content_object_sentiments.put(
        Body=(bytes(json.dumps({'reliable_sentiments': reliable_sentiments_json.to_dict(orient='records')}).encode('UTF-8') ) )
    )

if __name__ == '__main__' :

    start_time = time.time()

    main()
    
    end_time = time.time()
    execution_time_seconds = end_time - start_time
    minutes, seconds = divmod(execution_time_seconds, 60)
    print(f"INVESTING.COM crawling Script executed in {int(minutes)} minutes and {seconds:.2f} seconds.")