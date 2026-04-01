from __future__ import annotations


def prioritize_devices(devices: list[dict]) -> list[dict]:
    def is_primary(device: dict) -> bool:
        device_id = str(device.get("device_id") or "").lower()
        bundle_id = str(device.get("bundle_id") or "").lower()
        return "iphone" in device_id or "iphone" in bundle_id

    return sorted(devices, key=lambda item: 0 if is_primary(item) else 1)
