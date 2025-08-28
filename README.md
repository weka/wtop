# WTOP

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Issues](https://img.shields.io/github/issues/weka/wtop)](https://github.com/weka/wtop/issues)

**WTOP** (WEKA TOP) is an open source, real-time **terminal user interface (TUI)** for monitoring **WEKA performance metrics** across frontend and backend hosts.  
It provides live CPU, IOPS, throughput, and latency information in a top-like display, making it easy to monitor cluster health directly from the command line.

## Features

- 🖥️ **Interactive TUI** using [urwid](https://urwid.org/)  
- 📊 **Frontend & backend modes** for monitoring different host roles  
- ⚡ Real-time stats:
  - CPU usage
  - IOPS (Ops/s, Reads/s, Writes/s)
  - Latency (read/write in microseconds)
- 📑 Customizable columns  
- 🚀 Distributed as a **Python script** or as a **single-file binary**  

---

## Quick Start

Choose one method.

### Clone and run

```bash
git clone https://github.com/weka/wtop
cd wtop
./wtop
```

### Download binary directly
```
curl -L -o wtop https://raw.githubusercontent.com/weka/wtop/main/wtop
chmod +x wtop
./wtop
```

### Installation from Source

Clone the repository and install dependencies:
```
git clone https://github.com/weka/wtop.git
cd wtop/src
pip install -r requirements.txt
python3 wtop.py
```

## Usage

Run `wtop` in your terminal.

- Default mode shows **client (frontend) metrics**.  
- Switch to **backend view** with the appropriate key (see below).

If there is no active WEKA login then you will see this perpetually (resolve by issuing `weka user login`):
```
Status: Initializing...
Status: Fetching data...
```


### Keyboard Shortcuts

| Key       | Action                                |
|-----------|---------------------------------------|
| `q`       | Quit WTOP                             |
| `m`       | Switch modes between client and backend view|
| `h`       | Help / column descriptions            |


## Building from Source

To build a standalone binary with PyInstaller
```
pip install pyinstaller
pyinstaller --onefile wtop.py
```

The binary will be available in the dist/ directory:
```
./dist/wtop
```
## Requirements

- Python 3.6+

### Dependencies

- [urwid](https://urwid.org/)
- `json` (Python standard library)
- `csv` (Python standard library)

Install with:
```
pip install urwid
```
## Contributing

Pull requests are welcome! If you’d like to add new metrics, improve the UI, or extend backend integrations, please open an issue or PR.

## License

Licensed under the Apache License, Version 2.0
