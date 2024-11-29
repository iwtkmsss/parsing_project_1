import os
import time

import requests
import multiprocessing

from bs4 import BeautifulSoup

import pandas as pd


def format_execution_time(seconds):
    minutes = int(seconds // 60)
    seconds = seconds % 60
    return f"{minutes} minutes {seconds:.2f} seconds."


def append_to_csv(filename, data):
    if isinstance(data, dict):
        data = [data]

    df_to_append = pd.DataFrame(data)

    try:
        pd.read_csv(filename)
        df_to_append.to_csv(filename, mode='a', header=False, index=False)
    except FileNotFoundError:
        df_to_append.to_csv(filename, mode='w', header=True, index=False)


def scraping_main_page(main_url, headers):
    main_page_url = main_url
    all_categories = {}

    s = requests.Session()
    response = s.get(url=main_page_url, headers=headers)
    soup = BeautifulSoup(response.text, "lxml")
    all_cards = soup.find_all("li", class_="categories__list-item")

    for card in all_cards:
        card_name = card.find("a").text
        card_link = card.find("a")["href"]
        if "category" in card_link:
            all_categories[card_name] = card_link

    return all_categories


def scraping_sub_page(args):
    page_pagination, category, headers, main_url = args
    data = []
    print(page_pagination)

    s = requests.Session()
    response = s.get(url=page_pagination, headers=headers)
    soup = BeautifulSoup(response.text, "lxml")

    all_cards = soup.find_all("div", class_="products__list-item")
    for card in all_cards:
        product_availability = card.find("div", class_="products__item-stock in-stock")
        if product_availability is not None:
            product_availability = product_availability.text
        else:
            product_availability = "Немає в наявності"

        products__item = card.find("div", class_="products__item-name")
        product_link = products__item.find("a")["href"]
        product_name = products__item.find("a")

        if product_name is not None:
            product_name = product_name.text

        product_price = card.find("span", class_="price")

        if product_price is not None:
            product_price = product_price.text

        data.append({
            "Category": category,
            "Product Name": product_name,
            "Availability": product_availability,
            "Price": product_price,
            "Link": main_url + product_link
        })
    return data


def main():
    headers = {
        'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    }
    main_url = "https://goodtoys.com.ua"

    start_time = time.time()

    all_categories = scraping_main_page(main_url, headers)
    for index, category in enumerate(all_categories):
        start_time_pars = time.time()
        sub_page_url = main_url + all_categories[category]

        s = requests.Session()
        response = s.get(url=sub_page_url, headers=headers)
        soup = BeautifulSoup(response.text, "lxml")

        try:
            max_page = int(soup.find("ul", class_="menu-h").find_all("li")[-2].find("a").text)
        except AttributeError:
            max_page = 1

        tasks = [
            (sub_page_url + f"?page={page}", category, headers, main_url)
            for page in range(1, max_page + 1)
        ]

        with multiprocessing.Pool(processes=os.cpu_count()) as pool:
            print(f"\n[{category}] - starting scraped (page - {max_page}). [{index+1}/{len(all_categories)}]\n")
            results = pool.map(scraping_sub_page, tasks)
            print(f"\n[{category}] - successfully scraped.")

        print("Start saving data...")
        for result in results:
            append_to_csv('output.csv', result)
        print("Saving data is successfully...")
        execution_time_pars = format_execution_time(time.time() - start_time_pars)
        print(f"[{category}] [page - {max_page}] Collecting and saving data took: {execution_time_pars}")

    execution_time = format_execution_time(time.time()-start_time)
    print(f"\nAll categories were cleared and it took: {execution_time}")


if __name__ == '__main__':
    main()
# output.csv
