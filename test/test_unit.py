import io

import pytest

from returns.methods import unwrap_or_failure
from returns.pointfree import bind
from returns.pipeline import flow

from vnpay import VNPay, VNPayService, FTPRepo, BQRepo


@pytest.fixture()
def csv_data() -> io.BytesIO:
    with open("test\VNPAY.POS.csv", "rb") as f:
        return io.BytesIO(f.read())

@pytest.fixture()
def filename() -> str:
    return "VNPAY.POS (62).csv"

def test_list_files():
    with FTPRepo.FTP_CLIENT() as client:
        res = FTPRepo.list_files(client)
        assert res


def test_get_content():
    with FTPRepo.FTP_CLIENT() as client:
        res = FTPRepo.get_content(client)(FTPRepo.list_files(client)[0])
        res


def test_parse_data(csv_data):
    res = FTPRepo.parse_data(csv_data)
    res

def test_transform(csv_data):
    res = flow(
        csv_data,
        FTPRepo.parse_data,
        bind(VNPay.transform)
    )
    assert res

def test_load(csv_data):
    res = flow(
        csv_data,
        FTPRepo.parse_data,
        bind(VNPay.transform),
        bind(BQRepo.load(VNPayService.DATASET, VNPayService.TABLE, VNPay.schema))
    )
    assert res

def test_process(filename):
    with FTPRepo.FTP_CLIENT() as client:
        res = VNPayService.process_file(client)(filename)
        assert res
