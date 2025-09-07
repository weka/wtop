# WTOP

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Issues](https://img.shields.io/github/issues/weka/wtop)](https://github.com/weka/wtop/issues)

**WTOP** (WEKA TOP) is an open source, real-time terminal-based monitoring interface for WEKA cluster performance metrics, built with Python 3.6.8+ and urwid.

## Features

- **Real-time monitoring** of WEKA cluster performance metrics
- **Host-based rows** with configurable columns
- **Adjustable refresh rate** (default: 1 second)
- **Dynamic column management** - add, remove, and cycle through different metrics
- **Portable** - works on Linux hosts with Python 3.6.8+
- **Clean TUI interface** with color-coded metrics  
- 🚀 Distributed as a **Python script** or as a **single-file binary**  

## Requirements

- Python 3.6.8 or greater
- Linux host with WEKA CLI tools installed
- Access to execute `weka status` and `weka stats realtime` commands

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
### Command Line Options

The application runs with default settings and can be controlled entirely through the TUI interface.

## TUI Controls

### Navigation & Control
- **q** - Quit the application
- **h** - Show help dialog
- **+/-** - Increase/decrease refresh rate
- **Ctrl+C** - Quit (alternative)

### Column Management
- **1-9** - Cycle through available metrics for columns 1-9
- **a** - Add new column (with next available metric)
- **r** - Remove last column

### Available Metrics

The TUI monitors the following metrics from WEKA:

#### Real-time Stats (from `weka stats realtime`)
- **Node ID** - Node identifier
- **Hostname** - Host name
- **Writes/s** - Write operations per second
- **Write (B/s)** - Write bandwidth in bytes per second
- **Write Latency (µs)** - Write latency in microseconds
- **Reads/s** - Read operations per second
- **Read (B/s)** - Read bandwidth in bytes per second
- **Read Latency (µs)** - Read latency in microseconds
- **Ops/s** - Total operations per second
- **CPU %** - CPU utilization percentage
- **L6 Recv** - Level 6 receive metrics
- **L6 Sent** - Level 6 send metrics
- **OBS Upload** - Object storage upload metrics
- **OBS Download** - Object storage download metrics
- **RDMA Recv** - RDMA receive metrics
- **RDMA Sent** - RDMA send metrics
- **Roles** - Node roles
- **Mode** - Operation mode

#### Status Data (from `weka status -J`)
- **Total Ops** - Total operations count
- **Total Reads** - Total read operations
- **Total Writes** - Total write operations
- **Total Bytes Read** - Total bytes read
- **Total Bytes Written** - Total bytes written

## Default Layout

The application starts with these default columns:
1. Hostname
2. CPU %
3. Ops/s
4. Reads/s
5. Writes/s
6. Read Latency (µs)
7. Write Latency (µs)

## Customization

### Adding Columns
- Press **a** to add a new column with the next available metric
- The new column will appear on the right side

### Removing Columns
- Press **r** to remove the rightmost column
- At least one column (hostname) will always remain visible

### Cycling Metrics
- Press **1-9** to cycle through available metrics for that specific column
- Each column can display any available metric
- No duplicate metrics are allowed across columns

### Adjusting Refresh Rate
- Press **+** to increase refresh rate (max: 10 seconds)
- Press **-** to decrease refresh rate (min: 0.5 seconds)

## Data Sources

The TUI executes two WEKA commands to gather data:

1. **`weka status -J`** - Provides JSON output with cluster-wide activity statistics
2. **`weka stats realtime -F mode=client -f csv -R -v`** - Provides CSV output with per-host real-time performance metrics

## Error Handling

- **Command failures** are gracefully handled with error messages in the status bar
- **Network issues** or WEKA service problems are reported without crashing the application
- **Invalid data** is displayed as "N/A" rather than causing errors

## Troubleshooting

### Common Issues

1. **"Command not found: weka"**
   - Ensure WEKA CLI tools are installed and in PATH
   - Verify you have appropriate permissions to execute WEKA commands

2. **"Permission denied"**
   - Check that you have access to execute WEKA commands
   - Verify your user has appropriate cluster access

3. **No data displayed**
   - Check **weka status**
   - Verify network connectivity to cluster nodes
   - Check **weka service status**

4. **Display issues**
   - Ensure your terminal supports the required colors
   - Try resizing your terminal window

### Performance Considerations

- **Refresh rate**: Lower refresh rates (0.5-1s) provide more real-time data but increase system load
- **Column count**: More columns increase display complexity but provide more information
- **Network impact**: Each refresh executes WEKA commands, consider cluster load

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


### Dependencies

- [urwid](https://urwid.org/)
- `json` (Python standard library)
- `csv` (Python standard library)

Install with:
```
pip install urwid
```
## Contributing

This is a focused monitoring tool for WEKA clusters. Contributions are welcome for:
- Bug fixes
- Performance improvements
- Additional metric support
- Enhanced error handling

## License

Licensed under the Apache License, Version 2.0
