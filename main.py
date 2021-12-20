from vnpay.VNPayController import controller


def main(request) -> dict:
    request_json: dict = request.get_json()
    print(request_json)

    response = controller(request_json)
    print(response)

    return response
