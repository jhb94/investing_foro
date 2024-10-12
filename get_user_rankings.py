# Update: Todavia no este fino, pero por cada pais lee/geenra un json de sentimientos
# busca para cada empresa el ranking, filtra y busca los sentimietnos de esos usuarios.
# en la mayoria de paises no hace match, porque cada user es de un solo pais, pero cuando uno de los usuarios filtrados es del pais actual y hay una apuesta nuev ase manda el mail
# divido cada pais en un json para liberar la memoria
# iberdrola ha tardado 6 mins. en el ranking habia 1100 y despues del filtrado 2 maquinas.
# nvidia--> mins. ranking de 6133x8 y despues de filtrar 3 maquinas
# Con nvidia tarda la vida. No tiene sentido llamar al ranking para cada pais. Hay que cambiar esto

import requests
import cloudscraper
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
import pandas as pd
import json
import boto3
import calendar
from datetime import datetime
from typing import List
import time
import random
import logging
import utils.proxy_page_port_functionality as proxy_class


logging.basicConfig(filename="log_latest.log", level=logging.INFO)
logger = logging.getLogger()

scraper = cloudscraper.create_scraper()

# Initialize the BOTO3 client
s3 = boto3.resource('s3', region_name="eu-west-1")

# Initialize the SES client
ses_client = boto3.client("ses", region_name="eu-west-1")

## Rankings url, in the form of https://investing.com/common/sentiments/sentiments_ajax.php?action=get_user_rankings_bulk_records&item_ID=32237&sentimentsBulkCount=0
## Where sentimentsBulkCount is a paginator that groups 50 users per page.
# rankings_url = 'es.investing.com/common/sentiments/sentiments_ajax.php?action=get_user_rankings_bulk_records&item_ID='
# JB: separando por countries
rankings_url = '.investing.com/common/sentiments/sentiments_ajax.php?action=get_user_rankings_bulk_records&item_ID='

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
def get_user_ranking(identifier:str, country: str):

    # rankings_list_full = None


    iterator = 0
    response = ''
    records_left = True
    header_data = [['Rango', 'Usuario', 'Total',	'Cerrados',	'Ganadores','Gan. %','% Var.', 'UserLink']]

    while records_left:

        rankings_bulk_records = requests.get(f'https://{country}{rankings_url}{identifier}&sentimentsBulkCount={iterator}')
        # rankings_bulk_records = requests.get(f'https://{rankings_url}{identifier}&sentimentsBulkCount={iterator}')
        
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
                logger.info(f"Page {user_link} couldn't be loaded, status code: {user_equities_sentiments_html_page.status_code}")
                
                if user_equities_sentiments_html_page.status_code == 429 :
                    
                    logger.error("Rate Limit (429) exceeded, waiting 20s to retry....")
                    time.sleep(20)
                
                if user_equities_sentiments_html_page.status_code == 403 :
                    
                    logger.error("Getting FORBIDDEN (403) status responses, waiting 20s before retrying....")
                    time.sleep(20)
            # JB: por asegurar un sleep
            # time.sleep(5)
            retries -= 1

        except Exception as e:
            logger.error(f"Error processing GET request to 'https://www.investing.com{user_link}'. ERROR: {e}")
            

    ## TODO: Accept only 200
    soup = BeautifulSoup(user_equities_sentiments_html_page.text, "html.parser")

    sentiments_table = soup.find('table', attrs={'id':'sentiments_table'})
    
    if sentiments_table is None:
        logger.info(f"User data from link {user_link} could not be retrieved, sentiments object is NULL")
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
    # return user_sentiments_list.drop_duplicates(subset=['PredictionDate'], keep='first')
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
        logger.info("Email sent successfully.")
    else:
        logger.exception(  # pylint: disable=logging-fstring-interpolation
            f"Failed to send Email. Status Code: {response.status_code}, Response: {response.text}"
        )

def main ():
    
    ## Get some proxies in order to make the calls for us since AWS EC2 and service instances IP's are generally banned in cloudfare.
    ## spys.one has various different proxies depending on the country were they are located
    # proxies_url = 'https://spys.one/free-proxy-list/US/'
    proxies_url = 'https://spys.one/free-proxy-list/ES'

    proxies = proxy_class.get_proxies(proxies_url)

    ## READ INITIALIZATION FILES ##
    ## ------------------------- ##

    companies_file  = 'companies_to_watch.json'
    previous_sentiments_file = 'latest_reliable_sentiments.json'
    config_file = "config.json"

    bucket_name = 'investing.com-predictions-project-bucket'

    content_object_companies = s3.Object(bucket_name, companies_file)
    file_content_companies = content_object_companies.get()['Body'].read().decode('utf-8')

    companies_to_watch =  json.loads(file_content_companies)
    
    logger.info("Companies list file correctly loaded from S3")

    content_object_config = s3.Object(bucket_name, config_file)
    file_content_config = content_object_config.get()['Body'].read().decode('utf-8')

    app_config =  json.loads(file_content_config)

    logger.info("Configuration file correctly loaded from S3")

    ## Get current country, Process is run once per country every 2 hours.
    ## Most of them won't throw any results but being fast is not a priority 
    ## as long as the prediction arrives in less than 2 hours since posted
    ## GET current country and set the new one as the next in the list
    country = app_config["current_country"]

    if (app_config["countries"].index(country) + 1) == len(app_config["countries"]) : 

        next_country =  app_config["countries"][0]
    else:
        next_country =  app_config["countries"][app_config["countries"].index(country) + 1]

    app_config["current_country"] = next_country

    logger.info('-------------------')
    logger.info(f'----{country}---')
    logger.info('-------------------')
    
    ## We use a common json file with all countries prediction 
    previous_sentiments_file =  previous_sentiments_file

    try:
        s3.Object(bucket_name, previous_sentiments_file).load()
        logger.info("Previous sentiments file already exists in S3 and was correctly loaded now.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.info("No previous sentiments file found in bucket, creating a new one from scratch...")
            # Crear el archivo si no existe
            s3.Object(bucket_name, previous_sentiments_file).put(Body='')
            logger.info("Sentiments file initialized.")
        else:
            raise  # Re-lanza la excepción si es otro tipo de error

    content_object_sentiments = s3.Object(bucket_name, previous_sentiments_file)

    file_content_sentiments = content_object_sentiments.get()['Body'].read().decode('utf-8')

    initial_schema = {
        "reliable_sentiments": [
            {
                "PredictionDate": "",
                "Name": "",
                "Open": "",
                "% Var.": "",
                "DateForTheValue": "",
                "PredictionValue": ""
            }
        ]
    }
    if file_content_sentiments:
        previous_sentiments = json.loads(file_content_sentiments)
    else:
        previous_sentiments = initial_schema

    reliable_sentiments_json = pd.DataFrame(previous_sentiments["reliable_sentiments"])


    # TODO: que para cada country escriba su propio json

    for i in companies_to_watch["companies"]:

        identifier = i["identifier"]
        company_name = i["name"]
        win_percentage = i["win_percentage"]
        number_of_predictions = i["number_of_predictions"]
        variation_percentage = i["variation_percentage"]

        logger.info("Updating data predictions for company: %s, with identifier: %s", {company_name}, {identifier})

        ## Esta funcion se descarga la tabla de aquí como un dataframe:
        ## https://www.investing.com/equities/grupo-ezentis-sa-user-rankings
        rankings_list = get_user_ranking(identifier, country)

        logger.info(rankings_list)

        trusted_users = apply_trust_conditions(rankings_list,  win_percentage, number_of_predictions, variation_percentage)

        logger.info('-----------------------')

        logger.info('ADJUSTED RANKING OF USERS THAT MADE PREDICTIONS ON SYMBOL %s:', identifier)
        logger.info('PARAMETERS:')
        logger.info('WIN RATE : %s ', win_percentage)
        logger.info('TOTAL NUMBER OF PREDICTIONS : %s', number_of_predictions)
        logger.info('SUBYACENT VARIATION RATE : %s', win_percentage)

        logger.info('-----------------------')

        ## Aquí nos quedamos solo con los buenos, las trust conditions eliminan los cazurros
        logger.info(trusted_users)

        logger.info('Total of users : %s', len(trusted_users) )

        logger.info('-----------------------')

        logger.info('Looking for trustable users latest predictions')

        for _, trusted_user in trusted_users.iterrows():

            logger.info('Current user:  %s, \n Looking for user latest prediction...', {trusted_user['Usuario']})

            if trusted_user['UserLink'] is not None and trusted_user['UserLink'] != '' :

                last_user_prediction = find_latest_user_prediction_scrapper(trusted_user['UserLink'], company_name, proxies)

                # JB: un df vacio no es none
                # if not last_user_prediction:
                if last_user_prediction is None:
                    # JB: este next no hace lo que queremos. hay que usar continue
                    # next
                    continue
            elif trusted_user['UserLink'] is None:

                ## Format: members/200303883/sentiments-equities

                ## UNCOMMENT if user database in place
                # user_link = user_database[user_database['user_name'] == trusted_user['Usuario']]['user_id']
                # trusted_user['UserLink'] = f'/members/{user_link}/sentiments-equities'

                logger.info("User %s meets the requirements but has no link in this domain",trusted_user['Usuario'])
                continue
            
            if  last_user_prediction is not None:

                last_user_prediction['UserName'] = trusted_user['Usuario'] + trusted_user['UserLink'].replace('/members/', '(').replace('/sentiments-equities', ')')

                logger.info('Last prediction of user is: ')
                logger.info(last_user_prediction)
                logger.info('-----------------------')

                # Check if the new row exists in the JSON data
                reliable_sentiments_json_already_in_list = pd.concat([reliable_sentiments_json.astype(str), last_user_prediction.astype(str)], ignore_index=True)

                if not reliable_sentiments_json_already_in_list.duplicated().isin([True]).any():

                    logger.info("Sending information via email....")
                    send_email(last_user_prediction, app_config["emailFrom"] , app_config["emailTo"] )

                    logger.info("EMAIL SENT")

                    logger.info('Adding new sentiment ENTRY to the list')
                    reliable_sentiments_json = pd.concat([reliable_sentiments_json, last_user_prediction], ignore_index=True)

                else :
                    logger.info("Entry already exist in predictions JSON list")

            else:

                logger.info('-----------------------')
                logger.info('This user does not have any recent predictions')

    logger.info('Updating json file')

    reliable_sentiments_json = reliable_sentiments_json.astype(str)

    ## UPDATE S3 with new predictions found in case there are.
    content_object_sentiments.put(
        Body=(bytes(json.dumps({'reliable_sentiments': reliable_sentiments_json.to_dict(orient='records')}).encode('UTF-8') ) )
    )

    logger.info("Predictions file correctly updated in S3")

    content_object_config.put(
        Body=(bytes(json.dumps(app_config).encode('UTF-8')))
    )

    logger.info(f"Configuration file correctly updated in s3, next country in the loop is {next_country}")

if __name__ == '__main__' :

    start_time = time.time()

    main()
    
    end_time = time.time()
    execution_time_seconds = end_time - start_time
    minutes, seconds = divmod(execution_time_seconds, 60)
    print(f"INVESTING.COM crawling Script executed in {int(minutes)} minutes and {seconds:.2f} seconds.")