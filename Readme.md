Repository that monitores the behaviour of the best users in INVESTING.COM user forum.

The way it's done, and this functional idea can be applied to other forums behaving the same way:

1. Input the companies you want to put an aye on in the file companies_to_watch.json. Evey company in that configuration file has the following attribute structure:

    ````json
    {
                "identifier" : 32237, 
                "name": "Ezentis",
                "win_percentage" : 80,
                "number_of_predictions": 10,
                "variation_percentage" : 20
    }````

* identifier: This number can be obtained by inspecting the browser's network and noting down the ?identifier query string that is used in the AJAX call that retreives the user ranking
* name: Used inside the function find_latest_user_prediction_scrapper(), to filter those user predictions in the user page belonging to the company, this value can also be identified inspecting the browser's network.
* win_percentage: This is the result of user correct predictions / user total predictions, or how many times is the user correct out of all the tries.
* number_of_predictions: This parameter is used to filter out those users that do not have the correct level of depth.
* variation_percentage: How much % of benefit could that user have obtained by investing all the predictions made.

2. If restarting the application clean up the latest_reliable_sentiments.json file by leaving the realiable_sentiments object empty. If not restarting, leave as is.

    ````json 
    {"reliable_sentiments": []}
    `````

3. Run the get_user_rankings.py file. This file should be run on cron mode, f.e. every 15 minutes, and send the new alerts via email. This code has the following structure:

    3.1. Loop the companies in the json file. Apply below steps for every company.
    3.2. Get the user ranking for every company in the function get_user_ranking
    3.3 Apply trust conditions. In this function the input parameters set for every company are used to filter the rankings dataset and get only the best predictors. This is done in the apply_trust_conditions() function
    3.4 Execute the find_latest_user_prediction_scrapper() function, which will get, for every "trusted" user, the latest prediction made. If this prediction does exist in the latest_reliable_sentiments.json file, it will do nothing, if it doesn't exist, it will send an email with the predition (send_email()) and it will add it to the json file tracking the predictions.

    May 2024.

Below is shown a diagram with the design/ architecture of the full concept.

![Arquitecture](resources/Arqui.png)