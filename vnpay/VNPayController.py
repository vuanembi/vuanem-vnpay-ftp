from vnpay.VNPayService import pipeline


def controller(request_data: dict) -> dict:
    return {
        "controller": "pipeline",
        "results": pipeline(),
    }
