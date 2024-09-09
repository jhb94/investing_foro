import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import concurrent.futures
import time
import re
import random
import execjs
from typing import List

def get_xor_variables(script_string: str):
    # JavaScript code to decode the obfuscated variables
    js_code = f"""
    function evalScript(p, r, o, x, y, s) {{
        y = function(c) {{
            return (c < r ? '' : y(parseInt(c / r))) + ((c = c % r) > 35 ? String.fromCharCode(c + 29) : c.toString(36))
        }};
        if (!''.replace(/^/, String)) {{
            while (o--) {{
                s[y(o)] = x[o] || y(o)
            }}
            x = [function(y) {{
                return s[y]
            }}];
            y = function() {{
                return '\\\\w+'
            }};
            o = 1;
        }}
        while (o--) {{
            if (x[o]) {{
                p = p.replace(new RegExp('\\\\b' + y(o) + '\\\\b','g'), x[o])
            }}
        }}
        return p;
    }}
    var result = evalScript('{script_string}.split('\u005e'), 0, {{}});
    """

    # Step 4: Evaluate the JavaScript code and extract the variables
    ctx = execjs.compile(js_code)
    variables = ctx.eval('result')

    key_values = variables.split(';')

    key_values = list(filter(None, key_values))

    variable_dict = {}

    for key_value in key_values:
        variable, calculation = key_value.split('=')

        if '^' in calculation and calculation.split('^')[0].isdigit():
            value = int(calculation.split('^')[0])^int(calculation.split('^')[1])
        elif calculation.isdigit():
            value = int(calculation)
        else:
            value = variable_dict[calculation.split('^')[0]]^variable_dict[calculation.split('^')[1]]
        
        variable_dict[variable] = value

    return variable_dict

def get_substring_between(main_string: str, start_substring: str, end_substring: str):
    start_index = main_string.find(start_substring)
    if start_index == -1:
        return None  # Start substring not found
    end_index = main_string.find(end_substring, start_index + len(start_substring))
    if end_index == -1:
        return None  # End substring not found after start substring
    return main_string[start_index + len(start_substring):end_index]

def evaluate_xor_expression(expression:str, variables_dict: dict):
    # Extract variable names and XOR operations using regex
    matches = re.findall(r'(\w+)\^(\w+)', expression)
    
    result = ''
    for var1, var2 in matches:
        if var1 in variables_dict and var2 in variables_dict:
            xor_result = variables_dict[var1] ^ variables_dict[var2]
            result += str(xor_result)
        else:
            raise ValueError(f"Variable '{var1}' or '{var2}' not found in variables dictionary.")

    return result

## Generate proxy list parsing URL 
def get_proxies(proxies_url:str):

    proxies = []

    proxy_page = scraper.get(proxies_url)
    soup = BeautifulSoup(proxy_page.text, "html.parser")
    rows = soup.find_all('tr', class_='spy1xx') + soup.find_all('tr', class_='spy1x')

    ## Get the proxies script from XOR (p,r,o,x,y,s) function
    xor_script_string = get_substring_between(soup.find_all('script')[6].string, "return p}('", ".split(")

    xor_variables = get_xor_variables(xor_script_string)

    proxies = []
    for row in rows:

        try:

            # Extract the IP address
            ip = row.find_all('font', class_='spy14')[0].text.strip()                

            # Extract the proxy type, HTTP, HTTPS, 
            proxy_type = row.find_all('font', class_='spy1')[0].text
            
            if row.find_all('font', class_='spy14')[1].text == 'S':
                proxy_type += 's'

            # Extract the JavaScript expression
            script_tag = row.find('script')
            if script_tag:
                js_expr = script_tag.string.replace('document.write("<font class=spy2>:<\\/font>"+', '')
                port = evaluate_xor_expression(js_expr, xor_variables)
                if port:
                    proxies.append(f'{proxy_type.lower()}://{ip}:{port}')
                    
        except Exception as e:
            continue
    
    print('Using the following proxy list, only HTTP/HTTPS')
    print(proxies)
    return proxies

# Function to process a single member with retries and delay
def process_member(member:str, proxies: List[str]):
    retries = 2

    time.sleep(2)

    while retries > 0:
        try:

            proxy = random.choice(proxies)
            proxies = {"http": proxy}

            response = scraper.get(f'https://www.investing.com/members/{member}', timeout=10, proxies=proxies)
            
            if response.status_code != 200:
                print(f"User {member} not found. Status code: {response.status_code}")
                
                if response.status_code == 429 :
                    
                    print("Rate Limit exceeded, waiting 2 minutes to retry....")
                    time.sleep(120)
                
                if response.status_code == 403 :
                    
                    print("Getting FORBIDDEN status responses, waiting 2 min before retrying....")
                    time.sleep(120)

                return None    
            
            soup = BeautifulSoup(response.text, "html.parser")
            h1_tag = soup.find('h1', class_='float_lang_base_1')
            if h1_tag:
                return member, h1_tag.text
            return member, None
        except Exception as e:
            print(f"Error processing member {member}: {e}")
            retries -= 1

    return None

# Helper function to chunk the work
def chunked_worker(start_member:int, end_member: int, proxies:List[str]):
    local_data = []
    for member in range(start_member, end_member):
        result = process_member(member,proxies)
        if result:
            local_data.append(result)
            print(result)
    return local_data

start_time = time.time()

# Initialize an empty list to store the data
data = []

# Number of threads to use
num_threads = 10
max_members = 265000000
excluded_start = 162407
excluded_end = 200000000

# Calculate the total number of valid users
total_valid_users = (excluded_start - 1) + (max_members - excluded_end)
users_per_thread = total_valid_users // num_threads

# Define the ranges for each thread
ranges = []

range_count = 0
start_member = 1

for i in range(num_threads):
    if start_member < excluded_start:
        end_member = min(start_member + users_per_thread - 1, excluded_start - 1)
        if end_member >= excluded_start:
            end_member = excluded_start - 1
    else:
        start_member = excluded_end + range_count * users_per_thread
        end_member = min(start_member + users_per_thread - 1, max_members)
    
    range_count += 1

    if i == num_threads - 1:
        end_member = max_members
    
    ranges.append((start_member, end_member))
    start_member = end_member + 1

# Adjust ranges to ensure even distribution
if ranges[-1][1] != max_members:
    ranges[-1] = (ranges[-1][0], max_members)

scraper = cloudscraper.create_scraper()

## Europe Proxies:
# proxies_url = 'https://spys.one/europe-proxy/'

## USA Proxies:
proxies_url = 'https://spys.one/free-proxy-list/US/'

proxies = get_proxies(proxies_url)

with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    futures = []
    for start_member, end_member in ranges:
        futures.append(executor.submit(chunked_worker, start_member, end_member, proxies))

    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        if result:
            data.extend(result)

# Convert data to DataFrame
df = pd.DataFrame(data, columns=['user_id', 'user_name'])

# Save to CSV
df.to_csv('user_data.csv', index=False)

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Data has been saved to user_data.csv in {elapsed_time / 3600:.2f} hours.")
