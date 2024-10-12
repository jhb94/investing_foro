import cloudscraper
from bs4 import BeautifulSoup
import re
import execjs
import logging

logging.basicConfig(filename="log_latest.log", level=logging.INFO)
logger = logging.getLogger()

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

    logger.info("Variable dict correctly returned from XOR variable decryption")
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

    scraper = cloudscraper.create_scraper()

    proxy_page = scraper.get(proxies_url)
    soup = BeautifulSoup(proxy_page.text, "html.parser")
    rows = soup.find_all('tr', class_='spy1xx') + soup.find_all('tr', class_='spy1x')

    ## Get the proxies script from XOR (p,r,o,x,y,s) function
    xor_script_string = get_substring_between(soup.find_all('script')[4].string, "return p}('", ".split(")

    xor_variables = get_xor_variables(xor_script_string)

    proxies = []
    for row in rows:

        try:

            # Extract the IP address
            ip = row.find_all('font', class_='spy14')[0].text.strip()                

            # Extract the proxy type, HTTP, HTTPS, SOCKS5...
            proxy_type = row.find_all('td')[1].text.split()[0]
            if proxy_type == 'SOCKS5':
                proxy_type = 'HTTPS'

            # Extract the JavaScript expression
            script_tag = row.find('script')
            if script_tag:
                js_expr = script_tag.string.replace('document.write("<font class=spy2>:<\\/font>"+', '')
                port = evaluate_xor_expression(js_expr, xor_variables)
                if port:
                    proxies.append(f'{proxy_type.lower()}://{ip}:{port}')
                    
        except Exception as e:
            continue
    
    logger.info('Using the following proxy list, only HTTP/HTTPS')
    logger.info(proxies)
    return proxies
