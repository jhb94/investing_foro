import requests
import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import json
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime
import calendar

scraper = cloudscraper.create_scraper()

# Initialize the BOTO3 client
s3 = boto3.resource('s3', region_name="eu-west-1")

# Initialize the SES client
ses_client = boto3.client("ses", region_name="eu-west-1")

## identifier should be a console configurable input parameter
identifier = '32237' # EZENTIS: 32237

## Rankings url, in the form of https://investing.com/common/sentiments/sentiments_ajax.php?action=get_user_rankings_bulk_records&item_ID=32237&sentimentsBulkCount=0
## Where sentimentsBulkCount is a paginator that groups 50 users per page.
## Fixed param.
rankings_url = f'https://es.investing.com/common/sentiments/sentiments_ajax.php?action=get_user_rankings_bulk_records&item_ID='

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
def get_user_ranking(identifier:str):

    iterator = 0
    response = ''
    records_left = True
    header_data = [['Rango', 'Usuario', 'Total',	'Cerrados',	'Ganadores','Gan. %','% Var.', 'UserLink']]

    while records_left:
        
        rankings_bulk_records = requests.get(f'{rankings_url}{identifier}&sentimentsBulkCount={iterator}')

        if rankings_bulk_records.content == b'':
            records_left = False
        else:
            response += rankings_bulk_records.content.decode()
            iterator += 1
    
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

    ## Truncate user name to 19 characters as it appears in sentiments page, for later comparisson with that dataframe.
    # rankings_list['Usuario'] = rankings_list['Usuario'].str.slice(0,19)

    return rankings_list

def apply_trust_conditions(rankings_list : pd.DataFrame,  win_percentage : float, number_of_predictions: int, variation_percentage: float):

    adjusted_rankings = rankings_list[rankings_list['Gan. %'] >= win_percentage]
    adjusted_rankings = adjusted_rankings[adjusted_rankings['Total'] >= number_of_predictions]
    adjusted_rankings = adjusted_rankings[adjusted_rankings['% Var.'] >= variation_percentage]

    return adjusted_rankings

def find_latest_user_prediction_scrapper(user_link: str, company_name:str):

    user_equities_sentiments_html_page = scraper.get(f'http://es.investing.com{user_link}')

    soup = BeautifulSoup(user_equities_sentiments_html_page.text, "html.parser")

    sentiments_table = soup.find('table', attrs={'id':'sentiments_table'})
    
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
def send_email(last_user_prediction): 

    prediction = last_user_prediction.astype(str)

    corrective_actions_email_from = "onehck.internet@gmail.com"
    corrective_actions_email_to = ["onehck.internet@gmail.com"]
    corrective_actions_email_text = "New User prediction has been published: \n {prediction}" 
    corrective_actions_email_title="INVESTING.COM Alerting system, new prediction found: {Day}/{Month}/{Year}"


    msg = MIMEMultipart()

    # Fill the email template
    msg["From"] = corrective_actions_email_from
    msg["To"] = corrective_actions_email_to
    msg["Subject"] = corrective_actions_email_title.format(
        Year=datetime.now().year,
        Month=calendar.month_name[datetime.now().month],
        Day=datetime.now().day,
    )
    body = MIMEText(
        corrective_actions_email_text.format(
            Year=datetime.now().year,
            Month=calendar.month_name[datetime.now().month],
            Day=datetime.now().day,
            prediction=prediction
        )
    )
    msg.attach(body)

    # Send the email
    response = ses_client.send_raw_email(
        Source=corrective_actions_email_from,
        Destinations=[corrective_actions_email_to],
        RawMessage={"Data": msg.as_string()},
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

    for i in companies_to_watch["companies"]:

        identifier = i["identifier"]
        company_name = i["name"]
        win_percentage = i["win_percentage"]
        number_of_predictions = i["number_of_predictions"]
        variation_percentage = i["variation_percentage"]

        print("Updating data predictions for company: ", company_name , " with identifier: ", identifier)

        rankings_list = get_user_ranking(identifier)

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

            last_user_prediction = find_latest_user_prediction_scrapper(trusted_user['UserLink'], company_name)
            # last_user_prediction = find_latest_user_prediction(trusted_user['Usuario'])

            if not last_user_prediction.empty:

                print('Last prediction of user is: ')
                print('-----------------------')
                print(last_user_prediction)

                # send_email(last_user_prediction)

                # Check if the new row exists in the JSON data
                reliable_sentiments_json_already_in_list = pd.concat([reliable_sentiments_json.astype(str), last_user_prediction.astype(str)], ignore_index=True)

                if not reliable_sentiments_json_already_in_list.duplicated().isin([True]).any():

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
    
    # Write the updated DataFrame back to the JSON file
    # with open('latest_reliable_sentiments.json', 'w') as f:
    #     json.dump({'reliable_sentiments': reliable_sentiments_json.to_dict(orient='records')}, f)

if __name__ == '__main__' :

    main()
    
