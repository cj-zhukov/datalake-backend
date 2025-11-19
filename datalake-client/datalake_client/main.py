import argparse
from dataclasses import dataclass
from tqdm import tqdm
import logging
import requests
import time
import os
from datafusion import SessionContext
logging.basicConfig(level=logging.INFO)


URL = "path-to-api"
CHUNK_SIZE = 8192
CHECK_INTERVAL = 5  # secs
MAX_RETRIES = 1000  # ~80 min CHECK_INTERVAL * MAX_RETRIES = max time waiting for file


@dataclass
class ObjectStoreClient:
    query: str = "select * from 's3://path-to-data/data/' limit 100"
    path: str = "query"
    method: str = "post"
    output: str = "/tmp/download.parquet"

    def __post_init__(self):
        """Post-initialization checks."""
        if not self.path:
            raise ValueError("Path must be provided")
        if not self.query:
            raise ValueError("Query must be provided")
        if self.method.lower() not in {"post"}:
            raise ValueError(f"Unsupported HTTP method: {self.method}")

    def run(self) -> None:
        """Datalake-client main interface"""
        match self.path.lower():
            case "query":
                url = f"{URL}/query"
                presigned_url = self._send_request(url)
                if presigned_url is None:
                    return
                self._wait_for_result(presigned_url)
            case _:
                raise ValueError(f"Unsupported path: {self.path}")

    def _send_request(self, url: str) -> dict | str | None:
        """Sends a request and returns response or None."""
        payload = {"query": self.query}
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            return response.json().get("result_parquet")  # get url for parquet file
        except requests.HTTPError as http_err:
            logging.error(
                f"HTTP error: {http_err} | Status Code: {response.status_code}")
        except requests.RequestException as req_err:
            logging.error(f"Request error: {req_err}")
        return None

    def _wait_for_result(self, url: str, chunk_size=CHUNK_SIZE) -> None:
        """Polls until file is ready, then downloads and reads."""
        retries = 0
        while retries < MAX_RETRIES:
            if self._try_download(url):
                # self._read_with_datafusion()
                return
            time.sleep(CHECK_INTERVAL)
            retries += 1
        raise TimeoutError("Timed out waiting for file to become available.")

    def _try_download(self, url: str) -> bool:
        """Try to download Parquet file once."""
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200 and response.headers.get("Content-Length", "0") != "0":
                total_size = int(response.headers.get("Content-Length", 0))
                with open(self.output, "wb") as f, tqdm(
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    desc=f"Downloading {os.path.basename(self.output)}"
                ) as pbar:
                    for chunk in response.iter_content(CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
                logging.info(f"Downloaded Parquet file: {self.output}")
                return True
            else:
                logging.info("Backend still processing...")
                return False
        except requests.RequestException as e:
            logging.error(f"Download failed: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description="Datalake Client")
    parser.add_argument("-p", "--path", type=str, required=True, help="Path")
    parser.add_argument("-q", "--query", type=str,
                        required=True, help="SQL query")
    parser.add_argument("-o", "--output", type=str,
                        default="result.parquet", help="Output file path for download")
    args = parser.parse_args()
    client = ObjectStoreClient(
        query=args.query, path=args.path, method="post", output=args.output)
    client.run()


if __name__ == "__main__":
    main()
