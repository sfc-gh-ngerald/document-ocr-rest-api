import os
import json
import argparse

import requests

ENDPOINT = "ibbq3j-sfsenorthamerica-ngerald-aws2.snowflakecomputing.app"
DEFAULT_PAT_FILE = os.path.expanduser("~/.snowflake/tokens/.coco_desktop_pat_demo_aws2")


class OCRClient:
    def __init__(self, pat=None, pat_file=None):
        self._pat = pat or os.getenv("SNOWFLAKE_PAT")
        if not self._pat:
            path = pat_file or DEFAULT_PAT_FILE
            if os.path.isfile(path):
                self._pat = open(path).read().strip()
        if not self._pat:
            raise ValueError("No PAT found. Provide via argument, SNOWFLAKE_PAT env var, or ~/.snowflake/tokens/")

    def ocr(self, file_path):
        with open(file_path, "rb") as f:
            resp = requests.post(
                f"https://{ENDPOINT}/ocr",
                headers={"Authorization": f'Snowflake Token="{self._pat}"'},
                files={"file": (os.path.basename(file_path), f)},
            )
        resp.raise_for_status()
        return resp.json()


def main():
    parser = argparse.ArgumentParser(description="OCR documents via the SPCS endpoint")
    parser.add_argument("files", nargs="+", help="Path(s) to documents to OCR")
    parser.add_argument("--pat", default=os.getenv("SNOWFLAKE_PAT"), help="Programmatic Access Token")
    args = parser.parse_args()

    client = OCRClient(args.pat)
    for path in args.files:
        result = client.ocr(path)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
