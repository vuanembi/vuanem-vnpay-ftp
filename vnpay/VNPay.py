from typing import Optional
from datetime import datetime

import pytz
from returns.result import safe

TZ = pytz.timezone("Asia/Ho_Chi_Minh")


def parse_str(x: Optional[str]) -> Optional[str]:
    return str(x) if x else None


def parse_date(x: Optional[str]) -> Optional[str]:
    return (
        datetime.strptime(x, "%d/%m/%Y").replace(tzinfo=TZ).strftime("%Y-%m-%d")
        if x
        else None
    )


def parse_datetime(x: Optional[str]) -> Optional[str]:
    return (
        datetime.strptime(x, "%d/%m/%Y %H:%M:%S")
        .replace(tzinfo=TZ)
        .isoformat(timespec="seconds")
        if x
        else None
    )


def parse_float(x: Optional[str]) -> Optional[float]:
    return round(float(x.replace(",", "")), 6) if x else None


@safe
def transform(rows: list[dict]) -> list[dict]:
    return [
        {
            "stt": row["STT"],
            "ma_chuan_chi": row["Mã chuẩn chi"],
            "ma_tham_chieu": row["Mã tham chiếu"],
            "so_hoa_don": row["Số hóa đơn"],
            "ma_dai_ly": row["Mã đại lý"],
            "ten_dai_ly": row["Tên đại lý"],
            "ma_chi_nhanh": row["Mã chi nhánh"],
            "ten_chi_nhanh": row["Tên Chi Nhánh"],
            "mid": parse_str(row["MID"]),
            "tid": parse_str(row["TID"]),
            "mcc": parse_str(row["MCC"]),
            "ma_thiet_bi": row["Mã thiết bị"],
            "nh_xu_ly_giao_dich": row["NH xử lý giao dịch"],
            "so_tien_giao_dich": parse_float(row["Số tiền giao dịch"]),
            "ma_tien_te": row["Mã Tiền tệ"],
            "ngay_gio_giao_dich": parse_datetime(row["Ngày giờ giao dịch"]),
            "ngay_giao_dich": parse_date(row["Ngày giao dịch"]),
            "gio_giao_dich": row["Giờ giao dịch"],
            "ngay_gio_ket_toan": parse_datetime(row["Ngày giờ kết toán"]),
            "so_tien_phi_thu_ho_du_kien": parse_float(
                row["Số tiền phí thu hộ dự kiến"]
            ),
            "so_tien_phi_thu_ho_thuc_te": parse_float(
                row["Số tiền phí thu hộ thực tế"]
            ),
            "chenh_lech_phi_dai_ly": row["Chênh lệch phí đại lý"],
            "muc_phi_thuc_te": row["Mức phí % thực tế"],
            "muc_phi_co_dinh_thuc_te": row["Mức phí cố định thực tế"],
            "muc_phi_du_kien": row["Mức phí % dự kiến"],
            "muc_phi_co_dinh_du_kien": row["Mức phí cố định dự kiến"],
            "ngay_tinh_phi": parse_datetime(row["Ngày tính phí"]),
            "so_the": row["Số thẻ"],
            "loai_the": row["Loại thẻ"],
            "so_tai_khoan": row["Số tài khoản"],
            "chu_tai_khoan": row["Chủ tài khoản"],
            "ngan_hang": row["Ngân hàng"],
            "chi_nhanh": row["Chi nhánh"],
            "so_batch": row["Số Batch"],
            "loai_giao_dich": row["Loại giao dịch"],
            "ma_giao_dich": row["Mã giao dịch"],
            "hinh_thuc_giao_dich": row["Hình thức giao dịch"],
            "ngan_hang_phat_hanh": row["Ngân hàng phát hành"],
            "da_thanh_quyet_toan": row["Đã thanh quyết toán"],
            "ma_mdr": row["Mã MDR"],
            "ma_don_hang": row["Mã đơn hàng"],
            "ma_so_thue_dang_ky_kinh_doanh": row["Mã số thuế/ Đăng ký kinh doanh"],
            "vnpay_qr": row["VNPAY QR"],
            "trang_thai_hach_toan": row["Trạng thái hạch toán"],
        }
        for row in rows
    ]


schema = [
    {"name": "stt", "type": "NUMERIC"},
    {"name": "ma_chuan_chi", "type": "NUMERIC"},
    {"name": "ma_tham_chieu", "type": "NUMERIC"},
    {"name": "so_hoa_don", "type": "NUMERIC"},
    {"name": "ma_dai_ly", "type": "NUMERIC"},
    {"name": "ten_dai_ly", "type": "STRING"},
    {"name": "ma_chi_nhanh", "type": "NUMERIC"},
    {"name": "ten_chi_nhanh", "type": "STRING"},
    {"name": "mid", "type": "STRING"},
    {"name": "tid", "type": "STRING"},
    {"name": "mcc", "type": "STRING"},
    {"name": "ma_thiet_bi", "type": "STRING"},
    {"name": "nh_xu_ly_giao_dich", "type": "STRING"},
    {"name": "so_tien_giao_dich", "type": "NUMERIC"},
    {"name": "ma_tien_te", "type": "STRING"},
    {"name": "ngay_gio_giao_dich", "type": "TIMESTAMP"},
    {"name": "ngay_giao_dich", "type": "DATE"},
    {"name": "gio_giao_dich", "type": "STRING"},
    {"name": "ngay_gio_ket_toan", "type": "TIMESTAMP"},
    {"name": "so_tien_phi_thu_ho_du_kien", "type": "NUMERIC"},
    {"name": "so_tien_phi_thu_ho_thuc_te", "type": "NUMERIC"},
    {"name": "chenh_lech_phi_dai_ly", "type": "NUMERIC"},
    {"name": "muc_phi_thuc_te", "type": "NUMERIC"},
    {"name": "muc_phi_co_dinh_thuc_te", "type": "NUMERIC"},
    {"name": "muc_phi_du_kien", "type": "NUMERIC"},
    {"name": "muc_phi_co_dinh_du_kien", "type": "NUMERIC"},
    {"name": "ngay_tinh_phi", "type": "TIMESTAMP"},
    {"name": "so_the", "type": "STRING"},
    {"name": "loai_the", "type": "STRING"},
    {"name": "so_tai_khoan", "type": "STRING"},
    {"name": "chu_tai_khoan", "type": "STRING"},
    {"name": "ngan_hang", "type": "STRING"},
    {"name": "chi_nhanh", "type": "STRING"},
    {"name": "so_batch", "type": "NUMERIC"},
    {"name": "loai_giao_dich", "type": "STRING"},
    {"name": "ma_giao_dich", "type": "NUMERIC"},
    {"name": "hinh_thuc_giao_dich", "type": "NUMERIC"},
    {"name": "ngan_hang_phat_hanh", "type": "STRING"},
    {"name": "da_thanh_quyet_toan", "type": "BOOLEAN"},
    {"name": "ma_mdr", "type": "STRING"},
    {"name": "ma_don_hang", "type": "STRING"},
    {"name": "ma_so_thue_dang_ky_kinh_doanh", "type": "STRING"},
    {"name": "vnpay_qr", "type": "STRING"},
    {"name": "trang_thai_hach_toan", "type": "STRING"},
]
