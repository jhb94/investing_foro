import requests
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
import pandas as pd
import json
import boto3
import calendar
from datetime import datetime, timedelta
from typing import List
import time
import logging
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a handler that will rotate the logs every night. It will keep 3 days of logs in the instance.
handler = TimedRotatingFileHandler(
    "log_latest.log", when="midnight", interval=1, backupCount=3
)
handler.setLevel(logging.INFO)

# Format of logs.
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)


# Initialize the BOTO3 client
s3 = boto3.resource("s3", region_name="eu-west-1")

# Initialize the SES client
ses_client = boto3.client("ses", region_name="eu-west-1")

## Rankings url, in the form of https://investing.com/common/sentiments/sentiments_ajax.php?action=get_user_rankings_bulk_records&item_ID=32237&sentimentsBulkCount=0
## Where sentimentsBulkCount is a paginator that groups 50 users per page.
# rankings_url = 'es.investing.com/common/sentiments/sentiments_ajax.php?action=get_user_rankings_bulk_records&item_ID='
rankings_url = ".investing.com/common/sentiments/sentiments_ajax.php?action=get_user_rankings_bulk_records&item_ID="

## SAMPLE PAYLOAD: Note that some users may not have the link embedded in their name
"""
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
<tr>
    <td class="first left">2</td>
    <td class="left">Lola Castañas</td>
    <td>1</td>
    <td>1</td>
    <td>1</td>
    <td class="right">100</td>
    <td class="bold right greenFont">+3.23%</td>
</tr>
"""

## Below value is the separator that investing uses when the user inputs both end date and a predicted exact value
end_date_prediction_separator = " @ "

"""
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
"""


## Function that receives the identifier or symbol of a company and returns all the users that made predictions on it
def get_user_ranking(identifier: str, countries: List[str], profitability: float):

    header_data = [
        [
            "Rango",
            "Usuario",
            "Total",
            "Cerrados",
            "Ganadores",
            "Gan. %",
            "% Var.",
            "UserLink",
        ]
    ]
    rankings_list_full = None

    for country in countries:

        iterator = 0
        response = ""
        records_left = True

        try:

            while records_left:

                rankings_bulk_records = requests.get(
                    f"https://{country}{rankings_url}{identifier}&sentimentsBulkCount={iterator}"
                )

                ## In order to not iterate over all the rankings, we will check the last profitability that we get.
                ## We know that the rankings are ordered by profitability in desdending order.
                ## From certain point onwards it makes no sense to keep getting records.

                ## Example:
                ## '...td><td class="right">0</td><td class="bold right redFont">-1.35%</td></tr>'
                ## We need to get the -1.35 to see which is the latest rentability retrieved.
                cleaned_string = rankings_bulk_records.text.rstrip("</td></tr>")

                # Find the last occurrence of '>'
                last_number_str = cleaned_string[
                    cleaned_string.rfind(">") + 1 :
                ].replace("%", "")

                # Convert to float
                last_profitability = float(last_number_str)

                ## 403 forbidden
                ## 429 Rate limited.
                ## Exit While and go to next country in the for loop
                if rankings_bulk_records.status_code != 200:
                    break

                ## If there is no response break
                if rankings_bulk_records.content == b"":
                    records_left = False
                else:
                    response += rankings_bulk_records.content.decode()
                    iterator += 1

                ## If the users are not profitable break, exit While and go to next iteration in for loop (next country)
                if last_profitability < profitability:
                    break

            if len(response) == 0:
                next

        except Exception as e:
            logger.error(
                f"Error processing GET request to 'https://{country}{rankings_url}{identifier}&sentimentsBulkCount={iterator}'). ERROR: {e}"
            )

        soup = BeautifulSoup(response, "html.parser")

        table_data = [[cell.text for cell in row("td")] for row in soup("tr")]

        # Extracting links from 'a' tags, this links point to user pages.
        # Links are in the second td tag inside the href of the link
        links = [
            (
                row.find_all("td")[1].find("a")["href"]
                if row.find_all("td")[1].find("a")
                else None
            )
            for row in soup("tr")
        ]

        rows = [row + [link] for row, link in zip(table_data, links)]

        rankings = header_data + rows

        rankings_list = pd.DataFrame(rankings[1:], columns=rankings[0])

        rankings_list = rankings_list.astype(
            {"Total": "int32", "Cerrados": "int32", "Ganadores": "int32"}
        )

        rankings_list["% Var."] = (
            rankings_list["% Var."].str.replace("%", "", regex=False).astype(float)
        )
        rankings_list["Gan. %"] = rankings_list["Gan. %"].astype(float)
        rankings_list["UserLink"] = rankings_list["UserLink"].str.replace(
            "currencies", "equities"
        )

        rankings_list = rankings_list.dropna(subset=["UserLink"])

        if rankings_list_full is None:
            rankings_list_full = rankings_list
        else:
            rankings_list_full = pd.concat(
                [rankings_list_full, rankings_list], axis=0
            ).reset_index(drop=True)

    return rankings_list_full


## Filters out all user regarding trust conditions.
def apply_trust_conditions(
    rankings_list: pd.DataFrame,
    win_percentage: float,
    number_of_predictions: int,
    variation_percentage: float,
):

    adjusted_rankings = rankings_list[rankings_list["Gan. %"] >= win_percentage]
    adjusted_rankings = adjusted_rankings[
        adjusted_rankings["Total"] >= number_of_predictions
    ]
    adjusted_rankings = adjusted_rankings[
        adjusted_rankings["% Var."] >= variation_percentage
    ]

    return adjusted_rankings


def get_latest_company_predictions(identifier: str):

    ## Pure header hardcoding, this does not look very pro but it works.
    ## The idea is to repeat this call (group of calls taking the sentimentBulkCount) once every 6h per company
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "es-ES,es;q=0.9,en;q=0.8",
        "content-type": "application/x-www-form-urlencoded",
        "origin": "https://www.investing.com",
        "priority": "u=1, i",
        # 'referer': 'https://www.investing.com/equities/grupo-ezentis-sa-scoreboard',
        "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    data = {
        "action": "get_sentiments_rows",
        # 'pair_name': 'Grupo Ezentis SA',
        # 'last_value': '0.1270',
        # 'pair_decimal_change': '4',
        # 'pair_decimal_change_percent': '2',
        # 'pair_decimal_last': '4',
        # 'sentiments_category': 'pairs',
        "pair_ID": identifier,
        "sentimentsBulkCount": "0",
    }

    latest_predictions_full = None
    today = datetime.today()

    while True:

        response = requests.post(
            "https://www.investing.com/instruments/sentiments/recentsentimentsAjax",
            headers=headers,
            data=data,
        )

        latest_predictions = pd.DataFrame.from_records(
            json.loads(response.text)["rowsData"]
        )

        latest_predictions["change_percent"] = (
            latest_predictions["change_percent"]
            .str.replace("%", "", regex=False)
            .astype(float)
        )

        ## Get the last start_date, this allows us to stop the sentimentsBultCount incrementations and exit the loop
        ## Functionally this is equivalent to just getting the predictions older or equal to today
        last_day = pd.to_datetime(latest_predictions["start_date"].iloc[-1])

        ## Remove all the sentiments that do not have a specific forecast.
        latest_predictions = latest_predictions[
            latest_predictions["end_date"].str.contains("@")
        ]

        ## Convert Forecast column into 2 columns with expected value and date for the value:
        if latest_predictions.empty:
            latest_predictions["DateForTheValue"] = pd.Series(dtype="str")
            latest_predictions["PredictionValue"] = pd.Series(dtype="str")
        else:
            latest_predictions[["DateForTheValue", "PredictionValue"]] = (
                latest_predictions["end_date"].str.split(" @ ", expand=True)
            )

        latest_predictions = latest_predictions.drop(
            ["font_color", "var_tmp_plus"], axis=1
        )

        if latest_predictions_full is None:
            latest_predictions_full = latest_predictions
        else:
            latest_predictions_full = pd.concat(
                [latest_predictions_full, latest_predictions], axis=0, ignore_index=True
            )

        ## Get the next 50 sentiments
        data["sentimentsBulkCount"] = str(int(data["sentimentsBulkCount"]) + 1)

        ## If the predictions are from earlier days break. Give 1 day margin
        if last_day < (today - timedelta(days=5)):

            break

    return latest_predictions_full


## Send email to recipients with a prediction. Change email recipientes in config file.
def send_email(
    last_user_prediction: pd.DataFrame,
    prediction_notification_email_from: str,
    prediction_notification_email_to: List[str],
):

    CHARSET = "UTF-8"

    prediction_notification_email_text = "NEW USER PREDICTION PUBLISHED: \n"
    prediction_notification_email_title = (
        "INVESTING.COM Alerting system on: {Day}/{Month}/{Year}"
    )

    body = """\
        <html>
        <head></head>
        <body>
            {0}
        </body>
        </html>
        """.format(
        last_user_prediction.to_html()
    )

    # Send the email
    response = ses_client.send_email(
        Source=prediction_notification_email_from,
        Destination={
            "ToAddresses": [address for address in prediction_notification_email_to],
        },
        Message={
            "Body": {
                "Html": {
                    "Charset": CHARSET,
                    "Data": body,
                },
                "Text": {
                    "Charset": CHARSET,
                    "Data": prediction_notification_email_text,
                },
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": prediction_notification_email_title.format(
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


def main():

    ## READ INITIALIZATION FILES ##
    companies_file = "companies_to_watch.json"
    previous_sentiments_file = "latest_reliable_sentiments.json"
    config_file = "config.json"

    bucket_name = "investing.com-predictions-project-bucket"

    content_object_companies = s3.Object(bucket_name, companies_file)
    file_content_companies = (
        content_object_companies.get()["Body"].read().decode("utf-8")
    )

    companies_to_watch = json.loads(file_content_companies)

    logger.info("Companies list file correctly loaded from S3")

    content_object_config = s3.Object(bucket_name, config_file)
    file_content_config = content_object_config.get()["Body"].read().decode("utf-8")

    app_config = json.loads(file_content_config)

    logger.info("Configuration file correctly loaded from S3")

    ## We use a json file that contains all the previous predictions
    previous_sentiments_file = previous_sentiments_file

    try:
        s3.Object(bucket_name, previous_sentiments_file).load()
        logger.info(
            "Previous sentiments file already exists in S3 and was correctly loaded now."
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":

            logger.info(
                "No previous sentiments file found in bucket, creating a new one from scratch..."
            )

            ## Create sentiments file if it does not exist (happens only during first run)
            s3.Object(bucket_name, previous_sentiments_file).put(Body="")
            logger.info("Sentiments file initialized.")
        else:
            raise  # Re-raise exception if it is another type of error other than 404 not found

    content_object_sentiments = s3.Object(bucket_name, previous_sentiments_file)

    file_content_sentiments = (
        content_object_sentiments.get()["Body"].read().decode("utf-8")
    )

    initial_schema = {
        "reliable_sentiments": [
            {
                "PredictionDate": "",
                "Name": "",
                "Open": "",
                "% Var.": "",
                "DateForTheValue": "",
                "PredictionValue": "",
            }
        ]
    }
    if file_content_sentiments:
        previous_sentiments = json.loads(file_content_sentiments)
    else:
        previous_sentiments = initial_schema

    reliable_sentiments_json = pd.DataFrame(previous_sentiments["reliable_sentiments"])

    for i in companies_to_watch["companies"]:

        identifier = i["identifier"]
        company_name = i["name"]
        win_percentage = i["win_percentage"]
        number_of_predictions = i["number_of_predictions"]
        variation_percentage = i["variation_percentage"]

        logger.info(
            "Updating data predictions for company: %s, with identifier: %s",
            {company_name},
            {identifier},
        )

        ## Below function call downloads the sentiments-table from this url and saves it to a pandas dataframe object.
        ## https://www.investing.com/equities/grupo-ezentis-sa-user-rankings
        rankings_list = get_user_ranking(
            identifier=identifier,
            countries=app_config["countries"],
            profitability=variation_percentage,
        )

        if rankings_list.empty:
            logger.error(
                "Rankings list could not be retrieved for this company and country, skipping...."
            )
            continue

        logger.info(rankings_list)

        ## Below function call filters all the users that have put a sentiment on the company by the trust conditions.
        ## Users that make "good" predictions
        trusted_users = apply_trust_conditions(
            rankings_list, win_percentage, number_of_predictions, variation_percentage
        )

        logger.info("-----------------------")

        logger.info(
            "ADJUSTED RANKING OF USERS THAT MADE PREDICTIONS ON SYMBOL %s:", identifier
        )
        logger.info("PARAMETERS:")
        logger.info("WIN RATE : %s ", win_percentage)
        logger.info("TOTAL NUMBER OF PREDICTIONS : %s", number_of_predictions)
        logger.info("SUBYACENT VARIATION RATE : %s", variation_percentage)

        logger.info("-----------------------")
        logger.info(trusted_users)

        logger.info("Total of users : %s", len(trusted_users))

        logger.info("-----------------------")

        logger.info("Looking for trustable users latest predictions")

        latest_company_predictions = get_latest_company_predictions(
            identifier=identifier
        )

        for _, trusted_user in trusted_users.iterrows():

            ## Initialise variable to None is important in order for the prediction to not be messed up between trusted users
            last_user_prediction = None

            logger.info(
                "Current user:  %s, \n Looking for user latest prediction...",
                {trusted_user["Usuario"]},
            )

            if trusted_user["UserLink"] is not None and trusted_user["UserLink"] != "":

                ## Comparing the user_id value that we get from the latest company predictions and the user id embedded in the user link
                ## Being the later the user link column result from calling the user rankings for all the different domains (countries)
                last_user_prediction = latest_company_predictions[
                    latest_company_predictions["user_id"]
                    == trusted_user["UserLink"]
                    .replace("/members/", "")
                    .replace("/sentiments-equities", "")
                ]

            elif trusted_user["UserLink"] is None:

                ## Format: members/200303883/sentiments-equities

                logger.info(
                    "User %s meets the requirements but has no link",
                    trusted_user["Usuario"],
                )
                continue

            if not last_user_prediction.empty:

                logger.info("Last prediction of user is: ")
                logger.info(last_user_prediction)
                logger.info("-----------------------")

                ## Check if the new row exists in the JSON data file containing the previous prediction, to not write or send repeated predictions
                reliable_sentiments_json_already_in_list = pd.concat(
                    [
                        reliable_sentiments_json.astype(str),
                        last_user_prediction.astype(str),
                    ],
                    ignore_index=True,
                )

                if (
                    not reliable_sentiments_json_already_in_list.duplicated()
                    .isin([True])
                    .any()
                ):

                    logger.info("Sending information via email....")
                    send_email(
                        last_user_prediction,
                        app_config["emailFrom"],
                        app_config["emailTo"],
                    )

                    logger.info("EMAIL SENT")

                    logger.info("Adding new sentiment ENTRY to the list")
                    reliable_sentiments_json = pd.concat(
                        [reliable_sentiments_json, last_user_prediction],
                        ignore_index=True,
                    )

                else:
                    logger.info("Entry already exist in predictions JSON list")

            else:

                logger.info("-----------------------")
                logger.info("This user does not have any recent predictions")

    logger.info("Updating json file")

    reliable_sentiments_json = reliable_sentiments_json.astype(str)

    ## UPDATE S3 with new predictions found in case there are.
    content_object_sentiments.put(
        Body=(
            bytes(
                json.dumps(
                    {
                        "reliable_sentiments": reliable_sentiments_json.to_dict(
                            orient="records"
                        )
                    }
                ).encode("UTF-8")
            )
        )
    )

    logger.info("Predictions file correctly updated in S3")

    ## UPDATE S3 with the next country in the loop
    content_object_config.put(Body=(bytes(json.dumps(app_config).encode("UTF-8"))))

    logger.info(f"Configuration file correctly updated in s3")


if __name__ == "__main__":

    start_time = time.time()

    main()

    end_time = time.time()
    execution_time_seconds = end_time - start_time
    minutes, seconds = divmod(execution_time_seconds, 60)
    logger.info(
        f"INVESTING.COM crawling Script executed in {int(minutes)} minutes and {seconds:.2f} seconds."
    )
