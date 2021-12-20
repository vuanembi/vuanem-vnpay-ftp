import os
import io
import ftplib
import csv
from typing import Callable

from returns.io import IOResultE, impure_safe

SUCCESS_DIR = "success"
FAILURE_DIR = "failure"


def FTP_CLIENT() -> ftplib.FTP:
    client = ftplib.FTP(
        host=os.getenv("FTP_HOST", ""),
        user=os.getenv("FTP_USER", ""),
        passwd=os.getenv("FTP_PWD", ""),
    )
    client.encoding = "utf-8"
    return client


def list_files(client: ftplib.FTP) -> list[str]:
    return [i for i in client.nlst() if i not in (SUCCESS_DIR, FAILURE_DIR)]


def get_content(client: ftplib.FTP) -> Callable[[str], IOResultE[io.BytesIO]]:
    @impure_safe
    def get(filename: str) -> io.BytesIO:
        output = io.BytesIO()
        client.retrbinary(f"RETR {filename}", output.write)
        return output

    return get


@impure_safe
def parse_data(output: io.BytesIO) -> list[dict]:
    output.seek(0)
    return [i for i in csv.DictReader(io.StringIO(output.read().decode("utf-8-sig")))]
