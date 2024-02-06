from dotenv import load_dotenv
import os
import pandas as pd
import requests
from urllib.parse import urljoin


def configure():
    load_dotenv()

"""
Get a URL for each page of content. It's important to specify the amount of pages 
from which we want to retrieve the articles. This number should be kept below 500 per day due to the API's limitation.
Refer to the Developer section at https://open-platform.theguardian.com/access/ for more information.
"""
def get_page_url(url: str, page_amount: int = None) -> list[str]:
    if page_amount is None:
        response = requests.get(url).json()
        page_amount = response["response"]["pages"]
    pages_urls = []
    for i in range(1, page_amount+1):
        each_page_url = url + f"&page={i}"
        pages_urls.append(each_page_url)
    return pages_urls


# Get a json for each page of content.
def get_page_content(urls: list[str]) -> list[dict]:
    pages_content = []
    for url in urls:
        response = requests.get(url).json()
        pages_content.append(response)
    return pages_content


def create_dataframe(pages_content: list[dict]) -> pd.DataFrame:
    data = []
    # All the columns for the CSV file except the Wordcount field.
    column_names = ["id", "type", "sectionId", "sectionName", "webPublicationDate", "webTitle", "webUrl", "apiUrl",
                    "isHosted", "pillarId", "pillarName"]
    # The try-except block is necessary in case there are fewer articles on the last page than `pageSize`.
    try:
        for i in range(len(pages_content)):
            response_field = pages_content[i]["response"]
            for j in range(response_field["pageSize"]):
                results_field = response_field["results"][j]
                content_fields = {name: results_field[name] for name in column_names}
                content_fields["Wordcount"] = results_field["fields"]["wordcount"]
                data.append(content_fields)
    except IndexError:
        print("Data is prepared")

    df = pd.DataFrame(data)
    return df


def modify_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["webPublicationDate"] = pd.to_datetime(df["webPublicationDate"])

    df["year"] = df["webPublicationDate"].dt.year
    df.insert(5, "year", df.pop("year"))

    df = df.sort_values(by="webPublicationDate", ascending=False)
    df["formatted_date"] = df["webPublicationDate"].dt.strftime('%d/%m/%Y')

    df.insert(5, "formatted_date", df.pop("formatted_date"))

    df["Wordcount"] = pd.to_numeric(df["Wordcount"], errors="coerce")
    df = df[df["Wordcount"] >= 1000]

    df = df.reset_index(drop=True)
    return df


def main():
    configure()

    base_url = "https://content.guardianapis.com/search"
    query_params = {
        "q": "elections OR Brexit",
        "api-key": os.getenv("API_KEY"),
        "show-fields": "wordcount"
    }

    main_url = urljoin(base_url, "?" + "&".join([f"{key}={value}" for key, value in query_params.items()]))

    pages_urls = get_page_url(url=main_url, page_amount=3)
    pages_content = get_page_content(urls=pages_urls)

    raw_df = create_dataframe(pages_content)
    df = modify_dataframe(df=raw_df)
    df.to_csv("data.csv", index=False)

if __name__ == "__main__":
    main()