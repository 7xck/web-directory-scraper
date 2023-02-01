import boto3
import pandas as pd
import os
import time
import requests
from tqdm import tqdm

df = pd.read_csv("all_files_urls.csv")
BUCKET_NAME = "quant-arb-data"  # replace with the bucket you created
BASE_URL = "https://b.scsi.to/"
client = boto3.client("s3")

# READ!!!
# You probably want to create this completed/complete_urls.csv file in your bucket
# so that it resolves without error. Create a folder called "completed"
# then upload an empty csv with one column called "url". Simple!
def update_complete_urls():
    # get already downloaded files
    try:
        client.download_file(
            BUCKET_NAME, "completed/complete_urls.csv", "complete_urls.csv"
        )
        complete_urls = pd.read_csv("complete_urls.csv")["url"].tolist()
    except Exception as exc:
        print("Fetching completed files failed, must be empty", exc)
    return complete_urls


def upload_file_to_s3(session, *, bucket, key, filename):
    """
    Upload a file to S3 with a progress bar.

    From https://alexwlchan.net/2021/04/s3-progress-bars/
    """
    file_size = os.stat(filename).st_size

    s3 = session.client("s3")

    with tqdm.tqdm(total=file_size, unit="B", unit_scale=True, desc=filename) as pbar:
        s3.upload_file(
            Filename=filename,
            Bucket=bucket,
            Key=key,
            Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
        )


def download(url: str, fname: str, chunk_size=1024):
    resp = requests.get(url, stream=True)
    if resp.status_code != 200:
        raise Exception(f"Error: {resp.status_code}")
    total = int(resp.headers.get("content-length", 0))
    with open(fname, "wb") as file, tqdm(
        desc=fname,
        total=total,
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in resp.iter_content(chunk_size=chunk_size):
            size = file.write(data)
            bar.update(size)


def download_file(url):
    # parse a folder structure from url
    directory_and_file_name = url.replace(BASE_URL, "")
    file_name = directory_and_file_name.split("/")[-1]

    print("Downloading file ...")
    try:
        # download file
        download(url, fname="downloads/" + file_name, chunk_size=1024)
    except Exception as e:
        print("ERROR DOWNLOADING FILE: ", e)
        return

    # upload downloaded file to s3
    print("Uploading file to s3 ...", directory_and_file_name)
    with open("downloads/" + file_name, "rb") as f:
        client.upload_fileobj(f, BUCKET_NAME, f"{directory_and_file_name}")

    # record downloaded file
    print("recording that we've downloaded this file in 'completed' ...")
    client.download_file(
        BUCKET_NAME, "completed/complete_urls.csv", "complete_urls.csv"
    )
    complete_urls = pd.read_csv("complete_urls.csv")["url"].tolist()
    complete_urls.append(url)
    client.put_object(
        Body=bytes(
            pd.DataFrame(complete_urls, columns=["url"]).to_csv(), encoding="utf-8"
        ),
        Bucket=BUCKET_NAME,
        Key=f"completed/complete_urls.csv",
    )
    pd.DataFrame(complete_urls, columns=["url"]).to_csv("local_copy.csv")

    # delete file
    print("Deleting file from downloads folder ...", file_name)
    os.remove("downloads/" + file_name)


def main():
    complete_urls = update_complete_urls()
    df = df[~df["url"].isin(complete_urls)]
    url_list = df["url"].tolist()
    for url in url_list:
        # refresh downloaded list
        complete_urls = update_complete_urls()
        if url in complete_urls:
            print("Found already completed URL, skipping")
            continue

        print("DOWNLOADING FILE FROM: ", url)
        download_file(url)
        print("\n\n")
        time.sleep(5)


if __name__ == "__main__":
    main()
