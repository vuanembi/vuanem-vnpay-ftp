import ftplib

from returns.io import IOResultE, IOSuccess, IOFailure
from returns.pointfree import bind, lash
from returns.functions import tap
from returns.methods import unwrap_or_failure
from returns.unsafe import unsafe_perform_io
from returns.pipeline import flow

from vnpay import VNPay, FTPRepo, BQRepo

DATASET = "IP_VNPay"
TABLE = "POS"


def pipeline() -> list[dict]:
    with FTPRepo.FTP_CLIENT() as client:
        return [
            unsafe_perform_io(unwrap_or_failure(process_file(client)(i)))
            for i in FTPRepo.list_files(client)
        ]


def process_file(client: ftplib.FTP):
    def process(filename: str) -> IOResultE[dict]:
        return flow(
            filename,
            FTPRepo.get_content(client),
            bind(FTPRepo.parse_data),
            bind(VNPay.transform),
            bind(BQRepo.load(DATASET, TABLE, VNPay.schema)),
            bind(lambda x: IOSuccess({"table": TABLE, "output_rows": x})),
            tap(bind(FTPRepo.mv_to_success(client, filename))),
            tap(lash(FTPRepo.mv_to_failure(client, filename))),
            lash(lambda x: IOFailure({"table": TABLE, "error": x.message})),
        )

    return process
