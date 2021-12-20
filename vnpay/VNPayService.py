import ftplib

from returns.pointfree import bind
from returns.pipeline import flow

from vnpay import VNPay, FTPRepo, BQRepo

DATASET = "IP_VNPay"
TABLE = "POS"


def process_file(client: ftplib.FTP):
    def process(filename: str):
        return flow(
            filename,
            FTPRepo.get_content(client),
            bind(FTPRepo.parse_data),
            bind(VNPay.transform),
            bind(BQRepo.load(DATASET, TABLE, VNPay.schema)),
        )

    return process
