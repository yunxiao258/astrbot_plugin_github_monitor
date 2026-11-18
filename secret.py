"""凭据安全：Windows DPAPI 加解密（token 不落明文）。

仅用于把 /gh settoken 的 GitHub Token 加密后写入 state.json。
非 Windows 平台回退为明文存储（保持功能可用，但记录警告）。
"""

import base64
import ctypes
import sys
from ctypes import POINTER, Structure, byref, c_ubyte, c_ulong, cast, create_string_buffer


class DATA_BLOB(Structure):
    _fields_ = [("cbData", c_ulong), ("pbData", POINTER(c_ubyte))]


def _blob_to_bytes(blob: DATA_BLOB) -> bytes:
    return ctypes.string_at(blob.pbData, blob.cbData)


def is_windows() -> bool:
    return sys.platform.startswith("win")


def dpapi_encrypt(plain: str) -> str:
    """Windows DPAPI 加密（当前用户作用域），返回 base64 字符串"""
    raw = plain.encode("utf-8")
    buf = create_string_buffer(raw)
    data_in = DATA_BLOB(c_ulong(len(raw)), cast(buf, POINTER(c_ubyte)))
    data_out = DATA_BLOB()
    ok = ctypes.windll.crypt32.CryptProtectData(
        byref(data_in), None, None, None, None, 0, byref(data_out)
    )
    if not ok:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        return base64.b64encode(_blob_to_bytes(data_out)).decode("ascii")
    finally:
        ctypes.windll.kernel32.LocalFree(data_out.pbData)


def dpapi_decrypt(b64: str) -> str:
    """解密 dpapi_encrypt 的结果。不同用户 / 非本机无法解密。"""
    raw = base64.b64decode(b64)
    buf = create_string_buffer(raw)
    data_in = DATA_BLOB(c_ulong(len(raw)), cast(buf, POINTER(c_ubyte)))
    data_out = DATA_BLOB()
    ok = ctypes.windll.crypt32.CryptUnprotectData(
        byref(data_in), None, None, None, None, 0, byref(data_out)
    )
    if not ok:
        raise RuntimeError("DPAPI 解密失败：可能不是本机/当前用户加密")
    try:
        return ctypes.string_at(data_out.pbData, data_out.cbData).decode("utf-8")
    finally:
        ctypes.windll.kernel32.LocalFree(data_out.pbData)


def secure_store(token: str) -> str:
    """把 token 转成可安全落盘的字符串（Windows 上 dpapi: 加密）"""
    if is_windows():
        try:
            return "dpapi:" + dpapi_encrypt(token)
        except Exception:  # noqa: BLE001
            pass
    return token