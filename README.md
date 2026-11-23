![GrimReaper ESP32-C5](assets/icon/app_icon.png)
<h1 align="center">GrimReaper ESP32-C5</h1>

<p align="center">
  <strong>ESP32-C5 Wi-Fi Scanner & Deauther</strong>
  <br>
  Powered by ESP32-C5, Bluetooth LE and Flutter.
</p>

<p align="center">
  <em>One device. One application. One integrated monitoring platform.</em>
</p>

GrimReaper ESP32-C5 is an ESP32-C5-based Wi-Fi scanning and deauth project with a dedicated Flutter Android application. It combines embedded firmware, BLE communication, network discovery, saved network management, and an advanced device-side scheduling system into one integrated platform.

The project is designed around a simple idea: let the ESP32 handle the hardware-level operations while the Android application provides an intuitive interface for monitoring and controlling the device.

---

## ✨ Features

### 📡 Wi-Fi Scanning
- Discover nearby Wi-Fi access points.
- Display network information, including:
  - SSID
  - BSSID
  - Signal strength (RSSI)
  - Wi-Fi band
  - Channel
  - Security type
- Organize discovered networks by band and signal strength.

### 📱 Android Application
- Built with Flutter.
- Bluetooth Low Energy communication.
- Dedicated GrimReaper ESP32-C5 interface.
- Device connection and disconnection management.
- Live device status monitoring.
- Nearby and Saved Networks views.
- Automatic status updates.
- User-friendly controls and notifications.

### 💾 Saved Network Management
- Save discovered networks.
- Maintain a persistent saved network list on the ESP32.
- Display saved network information.
- Track network availability.
- Refresh saved network details following a Wi-Fi scan.

### ⏰ Advanced Scheduling System
- One-time schedules.
- Recurring schedules with selectable weekdays.
- Individual and all-saved-network action types.
- Start, Pause, Resume, and Stop controls.
- Global Pause All and Resume All controls.
- Emergency Stop All.
- Persistent schedule storage.
- Interruption stack for managing overlapping scheduled activities.
- Automatic resumption of interrupted activities.

### 🕒 Timezone Support
- Android phone time synchronization.
- Automatic phone timezone detection.
- 12-hour time display with AM/PM.
- ESP32 internal UTC timekeeping.
- POSIX timezone rules for local calendar calculations.

### 🔄 Persistent Storage
- Schedule configurations stored in ESP32 flash memory.
- Saved network information retained across restarts.
- Runtime state management and recovery.

---

## 🛠️ Hardware

| Component | Specification |
|---|---|
| Microcontroller | ESP32-C5 |
| Development Board | ESP32-C5-N16R8 |
| Flash | 16 MB |
| PSRAM | 8 MB |
| Wireless | Wi-Fi and Bluetooth LE |
| USB-to-Serial | CH340 (board dependent) |

---

## 💻 Software and Technologies

| Technology | Purpose |
|---|---|
| Arduino IDE | ESP32 firmware development |
| Arduino ESP32 Core | Embedded platform |
| C++ | ESP32 firmware |
| Flutter | Android application |
| Dart | Mobile application development |
| Bluetooth LE | ESP32-to-Android communication |
| Nordic UART Service | BLE communication protocol |

## 📲 Connecting to the ESP32-C5

1. Power on the ESP32-C5.
2. Open the GrimReaper ESP32-C5 Android application.
3. Enable Bluetooth on your Android device.
4. Find the GrimReaper BLE device.
5. Connect to the ESP32-C5.
6. Start scanning and monitor the discovered Wi-Fi networks.

The ESP32 communicates with the application through BLE notifications and commands.

---

## ⚙️ Scheduler Overview

The scheduler operates on the ESP32, allowing configured schedules to continue running independently of the Android application's foreground state while the device remains powered.

Supported scheduling capabilities include:

- One-time and recurring schedules.
- Active Duration and Fixed End Time modes.
- Configurable execution duration.
- Configurable pause intervals.
- Interruption and automatic resumption.
- Global pause, resume, and emergency stop.
- Persistent schedule configurations.

**Time synchronization:** After a complete ESP32 power loss or restart, the phone must synchronize the device clock before time-dependent schedules can execute reliably.

---

## 📱 Compatibility

- Android application developed using Flutter.
- Designed for Android devices supporting the required BLE functionality.
- Uses adaptive launcher icon resources for compatible Android launchers.
- Supports Android themed icons where the launcher and operating system provide support.

---

## 🔐 Permissions

The application may require Android permissions for:
- Bluetooth scanning.
- Bluetooth connections.
- Nearby devices.
- Location access on Android versions where required for BLE scanning.

Grant the permissions requested by Android when using the application.

---

## 🧑‍💻 Development

GrimReaper ESP32-C5 was developed using:
- ESP32 embedded firmware.
- Flutter and Dart.
- BLE communication.
- Persistent ESP32 storage.
- Device-side scheduling and time synchronization.

## 📜 License

This project is provided for educational and authorized testing purposes only.

---

## ⚠️ Legal Disclaimer
This project is published for defensive security research and education — penetration testing on networks you own or have explicit written permission to assess, security training, capture-the-flag exercises, lab demonstrations of Wi-Fi protocol weaknesses, and academic study.

Using deauthentication frames against networks you do not own or do not have explicit permission to test is illegal in most jurisdictions. Examples that are NOT covered by this license:

Interfering with a neighbor's, employer's, café's, school's, or airport's Wi-Fi
Disrupting other people's video calls, smart-home devices, or IoT equipment
Mass denial-of-service against arbitrary access points
Bypassing captive portals or evading paid Wi-Fi access controls
If you would not be comfortable showing the network owner exactly what you are doing, do not run this firmware against that network.

You are solely responsible for your use of this software. By building, flashing, or running it you accept that responsibility.

---

## ❤️ Acknowledgments

Developed through experimentation, embedded systems development, Bluetooth communication, and Flutter application engineering.

This project is inspired by these awesome projects:

[x1watt/esp32-c5-deauth](https://github.com/x1watt/esp32-c5-deauth)

[AnvilBrain/esp32-c5-dualband-deauther](https://github.com/AnvilBrain/esp32-c5-dualband-deauther)
