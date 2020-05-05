import pandas as pd
from splinter import Browser
from bs4 import BeautifulSoup
import requests

def main():
    #url to be scraped
    url = 'https://www.heritage.org/index/ranking'

    ########################################################
    # splinter method
    ########################################################
    #use chrome as browser and visit url/parse html from url
    executable_path = {'executable_path': 'chromedriver.exe'}
    browser = Browser('chrome', **executable_path, headless=True)
    browser.visit(url)
    html = browser.html
    splinter_soup = BeautifulSoup(html, 'html.parser')

    ########################################################
    # request method
    ########################################################
    #retrieve page and parse html from url
    response = requests.get(url)
    request_soup = BeautifulSoup(response.text, 'html.parser')
    #find all table data
    results = request_soup.find_all('td')

    splinter_tables = splinter_soup.find_all("td")
    request_tables = request_soup.find_all("td")

    assert (splinter_tables == request_tables)

    #create empty lists
    country_list, overall_list, change_list = ([] for i in range(3))

    #append result to a list depending on the td class is associated with
    for result in results:
        text = result.text.strip()
        if 'country' in result['class']:
            country_list.append(text)
        elif 'overall' in result['class']:
            overall_list.append(text)
        elif 'change' in result['class']:
            change_list.append(text)

    #convert to dataframe
    dict = {'country': country_list,'overall': overall_list,'change': change_list}
    rankings_df = pd.DataFrame(dict)

    browser.quit()

    return rankings_df

if __name__ == '__main__':
    main()