from dotenv import load_dotenv
import os
import pandas as pd
import requests


def configure():
    load_dotenv()


# Get a URL for each page of content.
def get_page_url(url: str) -> list[str]:
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
    for url in urls[0:2]:
        response = requests.get(url).json()
        pages_content.append(response)
    return pages_content


def create_dataframe(pages_content: list[dict]) -> pd.DataFrame:
    data = []
    # The try-except block is necessary in case there are fewer articles on the last page than `pageSize`.
    try:
        for i in range(len(pages_content)):
            for j in range(pages_content[i]["response"]["pageSize"]):
                content_fields = dict(
                    id=pages_content[i]["response"]["results"][j]["id"],
                    type=pages_content[i]["response"]["results"][j]["type"],
                    sectionId=pages_content[i]["response"]["results"][j]["sectionId"],
                    sectionName=pages_content[i]["response"]["results"][j]["sectionName"],
                    webPublicationDate=pages_content[i]["response"]["results"][j]["webPublicationDate"],
                    webTitle=pages_content[i]["response"]["results"][j]["webTitle"],
                    webUrl=pages_content[i]["response"]["results"][j]["webUrl"],
                    apiUrl=pages_content[i]["response"]["results"][j]["apiUrl"],
                    isHosted=pages_content[i]["response"]["results"][j]["isHosted"],
                    pillarId=pages_content[i]["response"]["results"][j]["pillarId"],
                    pillarName=pages_content[i]["response"]["results"][j]["pillarName"],
                    Wordcount=pages_content[i]["response"]["results"][j]["fields"]["wordcount"]
                )
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
    initial_url = (f"https://content.guardianapis.com/search?q=elections%20OR%20Brexit&api-key={os.getenv("API_KEY")}"
                   f"&show-fields=wordcount")
    pages_urls = get_page_url(url=initial_url)
    pages_content = get_page_content(urls=pages_urls)
    raw_df = create_dataframe(pages_content)
    df = modify_dataframe(df=raw_df)
    df.to_csv("data.csv", index=False)

if __name__ == "__main__":
    main()