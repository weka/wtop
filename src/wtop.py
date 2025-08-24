#!/usr/bin/env python3
"""
WEKA Performance Monitor TUI
A real-time terminal user interface for monitoring WEKA cluster performance.
"""

import urwid
import subprocess
import json
import csv
import signal
import sys
import math

class WekaMonitor:
    def __init__(self):
        """Initialize the WEKA Monitor"""
        # Data storage
        self.hosts = {}
        self.backend_hosts = {}  # Separate storage for backend hosts
        self.visible_columns = ['Hostname', 'CPU%', 'Ops/s', 'Reads/s', 'Writes/s', 'Read Latency(µs)', 'Write Latency(µs)']
        
        # Mode selection
        self.current_mode = 'client'  # 'client' or 'backend'
        
        # Available metrics mapping (CSV header -> Display name)
        self.available_metrics = {
            'Hostname': 'Hostname',
            'CPU%': 'CPU %',
            'Ops/s': 'Total Ops',
            'Reads/s': 'Reads/s',
            'Writes/s': 'Writes/s',
            'Read Latency(µs)': 'Read Latency (µs)',
            'Write Latency(µs)': 'Write Latency (µs)',
            'L6 Recv': 'L6 Recv',
            'L6 Sent': 'L6 Sent',
            'OBS Upload': 'OBS Upload',
            'OBS Download': 'OBS Download',
            'RDMA Recv': 'RDMA Recv',
            'RDMA Sent': 'RDMA Sent'
        }
        
        # Keep metric columns separate from fixed columns (ID, Hostname) so Hostname stays pinned
        self.metric_columns = self.get_unique_initial_metrics()
        self.refresh_rate = 1.0  # seconds
        self.cluster_status = {}
        self.selected_row = 0
        self.row_selection_mode = False
        self.row_selection_input = ""
        self.current_view = 'main'  # 'main' or 'node_details'
        self.selected_host = None
        self.node_details = {}
        self.showing_help = False
        
        # Sorting state
        self.sort_column = None
        self.sort_reverse = False
        
        # Sort state (no longer using sort menu)
        self.sort_column = None
        self.sort_reverse = False
        
        # Store main view metric configuration separately
        self.main_view_metric_columns = self.metric_columns.copy()
        
        # Role filter state for backend drill-down
        self.role_filters = {'DRIVES': True, 'COMPUTE': True, 'FRONTEND': True}  # All roles visible by default
        
        # Setup UI
        self.setup_ui()
        
        # Initialize data
        self.update_data()
        self.update_mode_display()
        
    def update_mode_display(self):
        """Update the mode display text"""
        mode_name = "Client" if self.current_mode == 'client' else "Backend"
        self.mode_text.set_text(f"Mode: {mode_name} (Press 'm' to switch)")
        
    def setup_ui(self):
        """Setup the user interface"""
        # Color palette
        palette = [
            ('header', 'white', 'dark blue'),
            ('status', 'white', 'dark green'),
            ('error', 'white', 'dark red'),
            ('help_header', 'white', 'dark blue'),
            ('help_text', 'white', 'black'),
            ('selected', 'black', 'light blue'),
            ('table_header', 'white', 'dark gray'),
            ('table_row', 'white', 'black'),
            ('table_row_alt', 'white', 'light gray'),
            ('footer', 'white', 'dark gray')
        ]
        
        # Header
        self.header = urwid.Text("WTOP the WEKA Performance Monitor - Press 'h' for help, 'm' to switch modes, 'q' to quit", align='left')
        header_attr = urwid.AttrMap(self.header, 'header')
        
        # Mode indicator
        self.mode_text = urwid.Text("Mode: Client", align='center')
        mode_attr = urwid.AttrMap(self.mode_text, 'status')
        
        # Cluster status
        self.cluster_status_text = urwid.Text("Status: Initializing...", align='left')
        cluster_status_attr = urwid.AttrMap(self.cluster_status_text, 'status')
        
        # Status bar
        self.status_text = urwid.Text("Status: Starting up...", align='left')
        status_attr = urwid.AttrMap(self.status_text, 'status')
        
        # Table
        self.table = urwid.ListBox(urwid.SimpleListWalker([]))
        
        # Footer
        self.footer_text = "Press 'h' for help, 'm' to switch modes, 'q' to quit"
        self.footer = urwid.Text(self.footer_text, align='center')
        footer_attr = urwid.AttrMap(self.footer, 'footer')
        
        # Main layout
        self.main_widget = urwid.Pile([
            ('pack', header_attr),
            ('pack', mode_attr),
            ('pack', cluster_status_attr),
            ('pack', status_attr),
            ('weight', 1, self.table),
            ('pack', footer_attr)
        ])
        
    def run(self):
        """Run the main application"""
        # Setup signal handlers
        def handle_sigint(signum, frame):
            raise urwid.ExitMainLoop()
            
        signal.signal(signal.SIGINT, handle_sigint)
        
        # Create main loop
        loop = urwid.MainLoop(self.main_widget, palette=[
            ('header', 'white', 'dark blue'),
            ('status', 'white', 'dark green'),
            ('error', 'white', 'dark red'),
            ('help_header', 'white', 'dark blue'),
            ('help_text', 'white', 'black'),
            ('selected', 'black', 'light blue'),
            ('table_header', 'white', 'dark gray'),
            ('table_row', 'white', 'black'),
            ('footer', 'white', 'dark gray')
        ])
        
        # Disable mouse support by setting mouse_tracking to False on the event loop
        if hasattr(loop, 'screen') and hasattr(loop.screen, 'set_mouse_tracking'):
            loop.screen.set_mouse_tracking(False)
        
        # Override unhandled_input to handle our custom input
        original_unhandled_input = loop.unhandled_input
        
        def custom_unhandled_input(key):
            if self.handle_input(key):
                return
            if original_unhandled_input:
                original_unhandled_input(key)
                
        loop.unhandled_input = custom_unhandled_input
        
        # Fetch initial data immediately instead of waiting
        self.update_data()
        self.update_display()
        self.update_cluster_status_display()
        
        # Schedule the first update with a shorter initial delay
        loop.set_alarm_in(0.5, self.update_data_and_display)
        
        # Start the main loop
        loop.run()
        
    def schedule_next_update(self, loop):
        """Schedule the next update using urwid's alarm system"""
        loop.set_alarm_in(self.refresh_rate, self.update_data_and_display)
        
    def update_data_and_display(self, loop, user_data):
        """Update data and display, then schedule next update"""
        try:
            # Don't update if help is showing
            if self.showing_help:
                # Schedule next update without updating data
                self.schedule_next_update(loop)
                return
                
            # Update data
            self.update_data()
            # Update display based on current view
            if self.current_view == 'node_details':
                # In node details view, only update the node details display
                self.update_node_details_display()
            else:
                # In main view, update the main display
                self.update_display()
            # Schedule next update
            self.schedule_next_update(loop)
        except Exception as e:
            # On error, try again in 2 seconds for faster recovery
            loop.set_alarm_in(2, self.update_data_and_display)
            
    def update_data(self):
        """Update data from WEKA commands"""
        try:
            # Get status data
            status_data = self.get_weka_status()
            
            # Get realtime stats
            stats_data = self.get_weka_stats()
            
            # Merge and update data
            self.merge_data(status_data, stats_data)
            
            # Check if we got meaningful data and adjust refresh rate accordingly
            if stats_data and len(stats_data) > 0:
                # We have data, only auto-adjust if refresh rate is very low (error recovery)
                if self.refresh_rate < 0.5:
                    self.refresh_rate = 0.5
                self.status_text.set_text("Status: Data updated successfully")
            else:
                # No data, use more aggressive refresh for faster data acquisition
                if self.refresh_rate > 0.5:
                    self.refresh_rate = 0.5
                self.status_text.set_text("Status: Fetching data...")
            
            # Only update cluster status if in main view
            if self.current_view != 'node_details':
                self.update_cluster_status_display()
            
            # Note: Node details are now handled in merge_data() to preserve sorting
                
        except Exception as e:
            # On error, temporarily increase refresh rate for faster recovery
            self.refresh_rate = min(self.refresh_rate + 0.5, 3.0)
            self.status_text.set_text(f"Status: Error updating data - {str(e)}")
                
    def get_weka_status(self):
        """Execute weka status -J and parse JSON output - optimized via selective parsing"""
        try:
            # Fetch full status data (no --fields option available)
            result = subprocess.run(
                ['weka', 'status', '-J'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                timeout=5  # Reduced timeout for faster failure detection
            )
            
            if result.returncode == 0:
                full_data = json.loads(result.stdout)
                
                # Optimize by extracting only the fields we need
                # This reduces memory usage and processing overhead
                optimized_data = {
                    'release': full_data.get('release'),
                    'name': full_data.get('name'),
                    'status': full_data.get('status'),
                    'capacity': full_data.get('capacity'),
                    'clients': full_data.get('clients'),
                    'io_nodes': full_data.get('io_nodes'),
                    'activity': full_data.get('activity'),
                    'active_alerts_count': full_data.get('active_alerts_count'),
                    'buckets': full_data.get('buckets'),
                }
                
                self.cluster_status = optimized_data  # Store optimized status data
                return optimized_data.get('activity', {})
            else:
                return {}
                
        except subprocess.TimeoutExpired:
            # Command timed out, return empty data but don't crash
            return {}
        except Exception as e:
            return {}
            
    def get_weka_stats(self):
        """Execute weka stats realtime and parse CSV output - dynamically optimized"""
        try:
            # Dynamically build output fields based on currently visible metrics
            # Always include hostname for grouping, then add only the metrics we're displaying
            output_fields = ['hostname']
            
            # For backend mode, include role to distinguish different process types
            if self.current_mode == 'backend':
                output_fields.append('role')  # Note: this becomes 'Roles' in CSV header
            
            # Map our display names to the actual CSV column names
            field_mapping = {
                'CPU%': 'cpu',
                'Ops/s': 'ops', 
                'Reads/s': 'readps',
                'Writes/s': 'writeps',
                'Read Latency(µs)': 'rlatency',
                'Write Latency(µs)': 'wlatency',
                'L6 Recv': 'l6recv',
                'L6 Sent': 'l6send',
                'OBS Upload': 'upload',
                'OBS Download': 'download',
                'RDMA Recv': 'rdmarecv',
                'RDMA Sent': 'rdmasend'
            }
            
            # Add only the fields for currently visible metrics
            for metric in self.metric_columns:
                if metric in field_mapping:
                    output_fields.append(field_mapping[metric])
            
            # Join fields for the -o parameter
            output_param = ','.join(output_fields)
            
            # Build the command with appropriate filters
            if self.current_mode == 'client':
                # Client mode: filter by mode=client and role=frontend
                cmd = [
                    'weka', 'stats', 'realtime', '-F', 'mode=client', '-F', 'role=frontend', 
                    '-f', 'csv', '-R', '-o', output_param
                ]
            else:
                # Backend mode: filter by mode=backend (no role filter to get all roles)
                cmd = [
                    'weka', 'stats', 'realtime', '-F', 'mode=backend', 
                    '-f', 'csv', '-R', '-o', output_param
                ]
            
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=5)
            
            if result.returncode == 0:
                if self.current_mode == 'backend':
                    parsed_data = self.parse_csv_stats_backend_aggregated(result.stdout)
                else:
                    parsed_data = self.parse_csv_stats_aggregated(result.stdout)
                return parsed_data
            else:
                return {}
                
        except subprocess.TimeoutExpired:
            # Command timed out, return empty data but don't crash
            return {}
        except Exception as e:
            return {}
            
    def get_host_node_details(self, hostname):
        """Get detailed node information for a specific host - dynamically optimized"""
        try:
            # Dynamically build output fields based on currently visible metrics
            # Always include node and hostname for identification, then add only the metrics we're displaying
            output_fields = ['node', 'hostname']
            
            # Map our display names to the actual CSV column names
            field_mapping = {
                'CPU%': 'cpu',
                'Ops/s': 'ops', 
                'Reads/s': 'readps',
                'Writes/s': 'writeps',
                'Read Latency(µs)': 'rlatency',
                'Write Latency(µs)': 'wlatency',
                'L6 Recv': 'l6recv',
                'L6 Sent': 'l6send',
                'OBS Upload': 'upload',
                'OBS Download': 'download',
                'RDMA Recv': 'rdmarecv',
                'RDMA Sent': 'rdmasend'
            }
            
            # Add only the fields for currently visible metrics
            for metric in self.metric_columns:
                if metric in field_mapping:
                    output_fields.append(field_mapping[metric])
            
            # Join fields for the -o parameter
            output_param = ','.join(output_fields)
            
            result = subprocess.run([
                'weka', 'stats', 'realtime', '-F', 'mode=client', '-F', f'hostname={hostname}', 
                '-f', 'csv', '-R', '-o', output_param
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=5)
            
            if result.returncode == 0:
                parsed_data = self.parse_node_details_csv(result.stdout)
                if not parsed_data:
                    print(f"Warning: No node details parsed for host {hostname}")
                return parsed_data
            else:
                print(f"Warning: weka stats command failed for host {hostname}: {result.stderr}")
                return []
                
        except subprocess.TimeoutExpired:
            # Command timed out, return empty data but don't crash
            return []
        except Exception as e:
            return []
            
    def get_backend_host_node_details(self, base_hostname):
        """Get detailed node information for a backend host showing all process types"""
        try:
            # Dynamically build output fields based on currently visible metrics
            # Always include node, hostname, and role for identification, then add only the metrics we're displaying
            output_fields = ['node', 'hostname', 'role']  # These become 'Node ID', 'Hostname', 'Roles' in CSV header
            
            # Map our display names to the actual CSV column names
            field_mapping = {
                'CPU%': 'cpu',
                'Ops/s': 'ops', 
                'Reads/s': 'readps',
                'Writes/s': 'writeps',
                'Read Latency(µs)': 'rlatency',
                'Write Latency(µs)': 'wlatency',
                'L6 Recv': 'l6recv',
                'L6 Sent': 'l6send',
                'OBS Upload': 'upload',
                'OBS Download': 'download',
                'RDMA Recv': 'rdmarecv',
                'RDMA Sent': 'rdmasend'
            }
            
            # Add only the fields for currently visible metrics
            for metric in self.metric_columns:
                if metric in field_mapping:
                    output_fields.append(field_mapping[metric])
            
            # Join fields for the -o parameter
            output_param = ','.join(output_fields)
            
            # Get all backend nodes for this base hostname (Frontend, Compute, Drives)
            # Don't filter by hostname here - get all backend data and filter during parsing
            result = subprocess.run([
                'weka', 'stats', 'realtime', '-F', 'mode=backend', 
                '-f', 'csv', '-R', '-o', output_param
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=5)
            
            if result.returncode == 0:
                parsed_data = self.parse_backend_node_details_csv(result.stdout, base_hostname)
                if not parsed_data:
                    print(f"Warning: No node details parsed for backend host {base_hostname}")
                return parsed_data
            else:
                print(f"Warning: weka stats command failed for backend host {base_hostname}: {result.stderr}")
                return []
                
        except subprocess.TimeoutExpired:
            # Command timed out, return empty data but don't crash
            return []
        except Exception as e:
            return []
            
    def parse_csv_stats_aggregated(self, csv_data):
        """Parse CSV output and aggregate metrics by host"""
        hosts_data = {}
        
        try:
            lines = csv_data.strip().split('\n')
            
            if len(lines) < 2:
                return hosts_data
                
            reader = csv.DictReader(lines)
            
            # Group data by hostname
            host_groups = {}
            for row in reader:
                hostname = row.get('Hostname', 'unknown')
                if hostname not in host_groups:
                    host_groups[hostname] = []
                
                # Convert values to proper types - use the actual CSV column names
                node_data = {
                    'Hostname': hostname,
                    'CPU%': self._to_float(row.get('CPU%', 0)),
                    'Ops/s': self._to_float(row.get('Ops/s', 0)),
                    'Reads/s': self._to_float(row.get('Reads/s', 0)),
                    'Writes/s': self._to_float(row.get('Writes/s', 0)),
                    'Read Latency(µs)': self._to_float(row.get('Read Latency(µs)', 0)),
                    'Write Latency(µs)': self._to_float(row.get('Write Latency(µs)', 0)),
                    'L6 Recv': self._to_float_bandwidth(row.get('L6 Recv', 0)),
                    'L6 Sent': self._to_float_bandwidth(row.get('L6 Sent', 0)),
                    'OBS Upload': self._to_float_bandwidth(row.get('OBS Upload', 0)),
                    'OBS Download': self._to_float_bandwidth(row.get('OBS Download', 0)),
                    'RDMA Recv': self._to_float_bandwidth(row.get('RDMA Recv', 0)),
                    'RDMA Sent': self._to_float_bandwidth(row.get('RDMA Sent', 0)),
                }
                host_groups[hostname].append(node_data)
            
            # Aggregate metrics for each host
            for hostname, nodes in host_groups.items():
                if not nodes:
                    continue
                    
                aggregated = {
                    'Hostname': hostname,
                    'CPU%': 0.0,
                    'Ops/s': 0.0,
                    'Reads/s': 0.0,
                    'Writes/s': 0.0,
                    'Read Latency(µs)': 0.0,
                    'Write Latency(µs)': 0.0,
                    'L6 Recv': 0.0,
                    'L6 Sent': 0.0,
                    'OBS Upload': 0.0,
                    'OBS Download': 0.0,
                    'RDMA Recv': 0.0,
                    'RDMA Sent': 0.0
                }
                
                node_count = len(nodes)
                for node in nodes:
                    # Sum ops/throughput metrics
                    for key in ['Ops/s', 'Reads/s', 'Writes/s', 'L6 Recv', 'L6 Sent', 'OBS Upload', 'OBS Download', 'RDMA Recv', 'RDMA Sent']:
                        if key in node and isinstance(node[key], (int, float)):
                            aggregated[key] += node[key]
                    
                    # Sum for averaging
                    for key in ['CPU%', 'Read Latency(µs)', 'Write Latency(µs)']:
                        if key in node and isinstance(node[key], (int, float)):
                            aggregated[key] += node[key]
                
                # Calculate averages for latency and CPU
                if node_count > 0:
                    for key in ['CPU%', 'Read Latency(µs)', 'Write Latency(µs)']:
                        if aggregated[key] > 0:
                            aggregated[key] /= node_count
                
                hosts_data[hostname] = aggregated
                
        except Exception as e:
            # If parsing fails, create empty data structure
            pass
            
        return hosts_data

    def parse_csv_stats_backend_aggregated(self, csv_data):
        """Parse CSV output for backend mode and aggregate metrics by host and process type"""
        hosts_data = {}
        
        try:
            lines = csv_data.strip().split('\n')
            
            if len(lines) < 2:
                return hosts_data
            
            reader = csv.DictReader(lines)
            
            # Group data by hostname and role (each role represents a different process type)
            host_groups = {}
            for row in reader:
                hostname = row.get('Hostname', 'unknown')
                role = row.get('Roles', row.get('role', 'unknown'))
                
                # Create unique key for each host-role combination
                host_key = f"{hostname}-{role}"
                
                if host_key not in host_groups:
                    host_groups[host_key] = []
                
                # Convert values to proper types - use the actual CSV column names
                node_data = {
                    'Hostname': host_key,
                    'BaseHostname': hostname,
                    'Role': role,
                    'CPU%': self._to_float(row.get('CPU%', 0)),
                    'Ops/s': self._to_float(row.get('Ops/s', 0)),
                    'Reads/s': self._to_float(row.get('Reads/s', 0)),
                    'Writes/s': self._to_float(row.get('Writes/s', 0)),
                    'Read Latency(µs)': self._to_float(row.get('Read Latency(µs)', 0)),
                    'Write Latency(µs)': self._to_float(row.get('Write Latency(µs)', 0)),
                    'L6 Recv': self._to_float_bandwidth(row.get('L6 Recv', 0)),
                    'L6 Sent': self._to_float_bandwidth(row.get('L6 Sent', 0)),
                    'OBS Upload': self._to_float_bandwidth(row.get('OBS Upload', 0)),
                    'OBS Download': self._to_float_bandwidth(row.get('OBS Download', 0)),
                    'RDMA Recv': self._to_float_bandwidth(row.get('RDMA Recv', 0)),
                    'RDMA Sent': self._to_float_bandwidth(row.get('RDMA Sent', 0)),
                }
                host_groups[host_key].append(node_data)
            
            # Aggregate metrics for each host-process type combination
            for host_key, nodes in host_groups.items():
                if not nodes:
                    continue
                    
                aggregated = {
                    'Hostname': host_key,
                    'BaseHostname': nodes[0]['BaseHostname'],
                    'Role': nodes[0]['Role'],
                    'CPU%': 0.0,
                    'Ops/s': 0.0,
                    'Reads/s': 0.0,
                    'Writes/s': 0.0,
                    'Read Latency(µs)': 0.0,
                    'Write Latency(µs)': 0.0,
                    'L6 Recv': 0.0,
                    'L6 Sent': 0.0,
                    'OBS Upload': 0.0,
                    'OBS Download': 0.0,
                    'RDMA Recv': 0.0,
                    'RDMA Sent': 0.0
                }
                
                node_count = len(nodes)
                for node in nodes:
                    # Sum ops/throughput metrics
                    for key in ['Ops/s', 'Reads/s', 'Writes/s', 'L6 Recv', 'L6 Sent', 'OBS Upload', 'OBS Download', 'RDMA Recv', 'RDMA Sent']:
                        if key in node and isinstance(node[key], (int, float)):
                            aggregated[key] += node[key]
                    
                    # Sum for averaging
                    for key in ['CPU%', 'Read Latency(µs)', 'Write Latency(µs)']:
                        if key in node and isinstance(node[key], (int, float)):
                            aggregated[key] += node[key]
                
                # Calculate averages for latency and CPU
                if node_count > 0:
                    for key in ['CPU%', 'Read Latency(µs)', 'Write Latency(µs)']:
                        if aggregated[key] > 0:
                            aggregated[key] /= node_count
                
                hosts_data[host_key] = aggregated
                
        except Exception as e:
            # If parsing fails, create empty data structure
            pass
            
        return hosts_data

    def sort_hosts(self, column):
        """Sort hosts by specified column"""
        if not self.hosts:
            return
            
        # Toggle sort direction if same column
        if self.sort_column == column:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = column
            self.sort_reverse = False
            
        # Convert hosts dict to list for sorting
        hosts_list = list(self.hosts.items())
        
        def sort_key(item):
            hostname, data = item
            value = data.get(column, 0)
            # Handle None/NaN values
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return float('-inf') if self.sort_reverse else float('inf')
            return value
            
        # Sort the list
        hosts_list.sort(key=sort_key, reverse=self.sort_reverse)
        
        # Rebuild hosts dict in sorted order
        self.hosts = dict(hosts_list)

    def sort_nodes(self, column):
        """Sort nodes within the selected host by specified column"""
        if not self.node_details:
            return
            
        # Toggle sort direction if same column
        if self.sort_column == column:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = column
            self.sort_reverse = False
            
        # Sort node_details list
        def sort_key(node):
            value = node.get(column, 0)
            # Handle None/NaN values
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return float('-inf') if self.sort_reverse else float('inf')
            return value
            
        self.node_details.sort(key=sort_key, reverse=self.sort_reverse)

    def _to_float(self, value):
        try:
            if value in (None, '', '0', 0):
                return 0.0
            return float(value)
        except Exception:
            try:
                cleaned = ''.join(ch for ch in str(value) if (ch.isdigit() or ch == '.' or ch == '-'))
                return float(cleaned) if cleaned else 0.0
            except Exception:
                return 0.0

    def _to_float_bandwidth(self, value):
        # Handles values like "0", "0 B/s", or plain numbers
        try:
            if value in (None, '', '0', 0):
                return 0.0
            s = str(value)
            if ' ' in s:
                s = s.split()[0]
            return float(s)
        except Exception:
            return 0.0
            
    def parse_node_details_csv(self, csv_data):
        """Parse CSV output for node details"""
        nodes = []
        
        try:
            lines = csv_data.strip().split('\n')
            
            if len(lines) < 2:
                return nodes
                
            reader = csv.DictReader(lines)
            
            for row in reader:
                # Robustly resolve node identifier: accept multiple possible header names
                node_id = (
                    row.get('Node ID')
                    or row.get('Node')
                    or row.get('node')
                    or row.get('NodeId')
                    or row.get('node_id')
                    or row.get('Node_Id')
                )
                if node_id in (None, ""):
                    node_id = 'N/A'

                node_data = {
                    'node': node_id,
                    'hostname': row.get('Hostname', 'N/A'),
                    'Writes/s': self._to_float(row.get('Writes/s', 0)),
                    'Write': self._to_float_bandwidth(row.get('Write', 0)),
                    'Write Latency(µs)': self._to_float(row.get('Write Latency(µs)', 0)),
                    'Reads/s': self._to_float(row.get('Reads/s', 0)),
                    'Read': self._to_float_bandwidth(row.get('Read', 0)),
                    'Read Latency(µs)': self._to_float(row.get('Read Latency(µs)', 0)),
                    'Ops/s': self._to_float(row.get('Ops/s', 0)),
                    'CPU%': self._to_float(row.get('CPU%', 0)),
                    'L6 Recv': self._to_float_bandwidth(row.get('L6 Recv', 0)),
                    'L6 Sent': self._to_float_bandwidth(row.get('L6 Sent', 0)),
                    'OBS Upload': self._to_float_bandwidth(row.get('OBS Upload', 0)),
                    'OBS Download': self._to_float_bandwidth(row.get('OBS Download', 0)),
                    'RDMA Recv': self._to_float_bandwidth(row.get('RDMA Recv', 0)),
                    'RDMA Sent': self._to_float_bandwidth(row.get('RDMA Sent', 0))
                }
                nodes.append(node_data)
                
        except Exception:
            pass
            
        return nodes
        
    def parse_backend_node_details_csv(self, csv_data, base_hostname):
        """Parse CSV output for backend node details, filtering by base hostname"""
        nodes = []
        
        try:
            lines = csv_data.strip().split('\n')
            
            if len(lines) < 2:
                return nodes
                
            reader = csv.DictReader(lines)
            
            total_rows = 0
            matching_rows = 0
            for row in reader:
                hostname = row.get('Hostname', 'unknown')
                
                # Only include rows for the specified base hostname
                # Check both exact match and if the hostname starts with base_hostname
                if hostname != base_hostname and not hostname.startswith(base_hostname):
                    continue
                
                # Get the role (process type) from the role column
                role = row.get('Roles', 'unknown')  # The CSV column is 'Roles'
                
                # Robustly resolve node identifier: accept multiple possible header names
                node_id = (
                    row.get('Node ID')
                    or row.get('Node')
                    or row.get('node')
                    or row.get('NodeId')
                    or row.get('node_id')
                    or row.get('Node_Id')
                )
                if node_id in (None, ""):
                    node_id = f"{hostname}-{role.capitalize()}" if role != 'unknown' else hostname

                node_data = {
                    'node': str(node_id),  # Use actual node ID from CSV
                    'hostname': hostname,
                    'role': role,
                    'Writes/s': self._to_float(row.get('Writes/s', 0)),
                    'Write': self._to_float_bandwidth(row.get('Write', 0)),
                    'Write Latency(µs)': self._to_float(row.get('Write Latency(µs)', 0)),
                    'Reads/s': self._to_float(row.get('Reads/s', 0)),
                    'Read': self._to_float_bandwidth(row.get('Read', 0)),
                    'Read Latency(µs)': self._to_float(row.get('Read Latency(µs)', 0)),
                    'Ops/s': self._to_float(row.get('Ops/s', 0)),
                    'CPU%': self._to_float(row.get('CPU%', 0)),
                    'L6 Recv': self._to_float_bandwidth(row.get('L6 Recv', 0)),
                    'L6 Sent': self._to_float_bandwidth(row.get('L6 Sent', 0)),
                    'OBS Upload': self._to_float_bandwidth(row.get('OBS Upload', 0)),
                    'OBS Download': self._to_float_bandwidth(row.get('OBS Download', 0)),
                    'RDMA Recv': self._to_float_bandwidth(row.get('RDMA Recv', 0)),
                    'RDMA Sent': self._to_float_bandwidth(row.get('RDMA Sent', 0))
                }
                nodes.append(node_data)
                
        except Exception as e:
            print(f"Error parsing backend node details CSV: {e}")
            pass
            
        return nodes
        
    def calculate_node_totals(self):
        """Calculate totals and averages for nodes"""
        if not self.node_details:
            return {}
            
        totals = {}
        
        # Initialize totals
        for key in self.node_details[0].keys():
            if key in ['node', 'hostname']:
                continue
            totals[key] = 0.0
            
        # Sum/average values
        for node in self.node_details:
            for key, value in node.items():
                if key in ['node', 'hostname']:
                    continue
                if isinstance(value, (int, float)):
                    totals[key] += value
                    
        # Calculate averages for latency and CPU
        node_count = len(self.node_details)
        if node_count > 0:
            # CPU% should be MAX value, not average
            if 'CPU%' in totals:
                cpu_values = [node.get('CPU%', 0) for node in self.node_details if isinstance(node.get('CPU%'), (int, float))]
                if cpu_values:
                    totals['CPU%'] = max(cpu_values)
            
            # Latency should be average
            for key in ['Read Latency(µs)', 'Write Latency(µs)']:
                if key in totals:
                    totals[key] /= node_count
                    
        return totals
        
    def merge_data(self, status_data, stats_data):
        """Merge status and stats data"""
        # Store the current sort state before updating
        current_sort_column = getattr(self, 'sort_column', None)
        current_sort_reverse = getattr(self, 'sort_reverse', False)
        
        # Update the hosts data based on current mode
        if self.current_mode == 'backend':
            self.backend_hosts = stats_data
            self.hosts = self.backend_hosts  # Use backend_hosts for display
        else:
            self.hosts = stats_data
        
        # Re-apply sorting if there was a previous sort
        if current_sort_column and self.hosts:
            # Check if the sort column exists in any of the host data
            sample_host_data = next(iter(self.hosts.values()), {})
            if current_sort_column in sample_host_data:
                self.sort_data(current_sort_column, not current_sort_reverse)
        
        # Always refresh drill-down view data when in that view
        if self.current_view == 'node_details' and self.selected_host:
            # Re-fetch node details with fresh data based on current mode
            if self.current_mode == 'backend' and '-' in self.selected_host:
                # Backend mode: extract base hostname and role, then filter
                parts = self.selected_host.split('-')
                if len(parts) >= 2:
                    base_hostname = '-'.join(parts[:-1])
                    selected_role = parts[-1]
                    all_nodes = self.get_backend_host_node_details(base_hostname)
                    # Show all process types for the base hostname
                    self.node_details = all_nodes
                else:
                    self.node_details = self.get_host_node_details(self.selected_host)
            else:
                # Client mode: use existing logic
                self.node_details = self.get_host_node_details(self.selected_host)
            
            # Re-apply sorting if there was a previous sort
            if current_sort_column and self.node_details:
                # Check if the sort column exists in any of the node data
                sample_node_data = next(iter(self.node_details), {})
                if current_sort_column in sample_node_data:
                    # Apply sorting directly to node details (EXCLUDE totals from sorting)
                    node_data_only = [node for node in self.node_details if 'node' in node]
                    node_data_only.sort(key=lambda x: x.get(current_sort_column, 0), reverse=current_sort_reverse)
                    # Reconstruct node_details with totals first, then sorted nodes
                    totals_row = [node for node in self.node_details if 'node' not in node]
                    self.node_details = totals_row + node_data_only
                
                # Note: Display update is handled by update_data_and_display() to avoid conflicts
            
    def update_cluster_status_display(self):
        """Update the cluster status display"""
        try:
            if not self.cluster_status:
                return
                
            # Extract cluster information
            version = self.cluster_status.get('release', 'Unknown')
            cluster_name = self.cluster_status.get('name', 'Unknown')
            # Note: protection_level doesn't exist in schema, using status instead
            protection = self.cluster_status.get('status', 'Unknown')
            
            # Extract capacity information
            capacity_info = self.cluster_status.get('capacity', {})
            total_capacity = capacity_info.get('total_bytes', 0)
            # Calculate used capacity: total - unprovisioned
            unprovisioned = capacity_info.get('unprovisioned_bytes', 0)
            used_capacity = total_capacity - unprovisioned
            
            # Extract client and node information
            clients_info = self.cluster_status.get('clients', {})
            active_clients = clients_info.get('active', 0) if isinstance(clients_info, dict) else 0
            io_nodes_info = self.cluster_status.get('io_nodes', {})
            active_io_nodes = io_nodes_info.get('active', 0) if isinstance(io_nodes_info, dict) else io_nodes_info
            
            # Extract activity information
            activity = self.cluster_status.get('activity', {})
            total_ops = activity.get('num_ops', 0)
            read_ops = activity.get('num_reads', 0)
            write_ops = activity.get('num_writes', 0)
            total_throughput = activity.get('sum_bytes_read', 0) + activity.get('sum_bytes_written', 0)
            read_throughput = activity.get('sum_bytes_read', 0)
            write_throughput = activity.get('sum_bytes_written', 0)
            
            # Extract alert information
            alert_count = self.cluster_status.get('active_alerts_count', 0)
            
            # Extract bucket information with null safety
            buckets_info = self.cluster_status.get('buckets', {})
            total_buckets = buckets_info.get('total', 0) or 0
            active_buckets = buckets_info.get('active', 0) or 0
            down_buckets = total_buckets - active_buckets
            
            # Format the display
            status_text = f"Release: {version} | Cluster: {cluster_name} | Status: {protection} | Capacity: {self.format_capacity(total_capacity)} | Used: {self.format_capacity(used_capacity)} | IO-Nodes: {active_io_nodes} | Buckets: {total_buckets} | Active Buckets: {active_buckets} | Down Buckets: {down_buckets} | Alerts: {alert_count}"
            status_text += f"\nOPS: {self.format_ops(total_ops)} (R:{self.format_ops(read_ops)} W:{self.format_ops(write_ops)}) | Throughput: {self.format_throughput(total_throughput)} (R:{self.format_throughput(read_throughput)} W:{self.format_throughput(write_throughput)})"
            # status_text += f"\nBuckets: {total_buckets} | Active Buckets: {active_buckets} | Down Buckets: {down_buckets}"
            
            self.cluster_status_text.set_text(status_text)
            
        except Exception as e:
            self.cluster_status_text.set_text(f"Status: Error parsing cluster data - {str(e)}")
            
    def update_display(self):
        """Update the main display"""
        if not self.hosts:
            return
            
        # Create table rows
        rows = []
        
        # Header row
        header_cells = ['ID', 'Hostname']
        # Add metric columns from self.metric_columns only (keeps Hostname pinned)
        for col in self.metric_columns:
            header_cells.append(col)
        
        # Build a columns spec that pins Hostname next to ID
        header_columns = []
        header_columns.append(('fixed', 3, urwid.AttrMap(urwid.Text('ID', align='center'), 'table_header')))
        header_columns.append(('fixed', 1, urwid.AttrMap(urwid.Text(''), 'table_header')))
        header_columns.append(('weight', 2, urwid.AttrMap(urwid.Text('Hostname', align='left'), 'table_header')))
        for col in self.metric_columns:
            header_columns.append(('weight', 1, urwid.AttrMap(urwid.Text(col, align='left'), 'table_header')))
        rows.append(urwid.Columns(header_columns))
        
        # Data rows
        host_list = list(self.hosts.items())
        for i, (hostname, data) in enumerate(host_list, 1):
            row_cells = [f"{i}"]
            
            # Add hostname (always second column)
            row_cells.append(hostname)
            
            # Add metric columns from self.metric_columns
            for col in self.metric_columns:
                value = data.get(col, 'N/A')
                
                if isinstance(value, (int, float)):
                    if col in ['CPU%', 'Read Latency(µs)', 'Write Latency(µs)']:
                        display_value = f"{value:.2f}"
                    elif col in ['Ops/s', 'Reads/s', 'Writes/s']:
                        display_value = self.format_ops(value)
                    elif col in ['L6 Recv', 'L6 Sent', 'OBS Upload', 'OBS Download', 'RDMA Recv', 'RDMA Sent']:
                        display_value = self.format_throughput(value)
                    else:
                        display_value = f"{value}"
                else:
                    display_value = str(value)
                    
                row_cells.append(display_value)
                
            # Build columns: ID fixed, small spacer fixed, Hostname weighted, metrics weighted
            row_columns = []
            row_columns.append(('fixed', 3, urwid.Text(row_cells[0], align='center')))
            row_columns.append(('fixed', 1, urwid.Text('')))
            row_columns.append(('weight', 2, urwid.Text(row_cells[1], align='left')))
            for val in row_cells[2:]:
                row_columns.append(('weight', 1, urwid.Text(val, align='left')))
            row = urwid.Columns(row_columns)
            
            # Apply styling: selection takes priority, then banded rows
            if i - 1 == self.selected_row:
                # Selected row gets priority styling
                row = urwid.AttrMap(row, 'selected')
            else:
                # Non-selected rows get banded styling
                if i % 2 == 0:
                    row = urwid.AttrMap(row, 'table_row_alt')
                else:
                    row = urwid.AttrMap(row, 'table_row')
                
            rows.append(row)
            
        # Update table
        self.table.body = urwid.SimpleListWalker(rows)
        
        # Update footer
        self.update_footer()

    def show_help_screen(self):
        """Display the help screen"""
        help_text = [
            "WTOP the WEKA Performance Monitor - Help",
            "",
            "Navigation & Control:",
            "q - Quit the application",
            "h - Show this help",
            "m - Switch between Client and Backend modes",
            "+/- - Increase/decrease refresh rate",
            "Ctrl+C - Quit (alternative)",
            "",
            "Column Management:",
            "1-9 - Cycle through metrics for columns 1-9",
            "a - Add new column (with next available metric)",
            "r - Remove last column",
            "",
            "Row Selection & Drill-down:",
            ": <number> - Quick select row by number (e.g., :3)",
            ": <number>d - Quick select and drill into row (e.g., :3d)",
            "Enter - Drill down to node details for selected host",
            "↑/↓ - Navigate between rows",
            "Escape - Return from node details to main view",
            "",
            "Sorting:",
            ":s+<column> - Sort by column in ascending order (e.g., :s+3, :s+CPU%)",
            ":s-<column> - Sort by column in descending order (e.g., :s-3, :s-Ops/s)",
            "",
            "Available Metrics:",
            "• Performance: CPU%, Ops/s, Reads/s, Writes/s",
            "• Latency: Read/Write Latency (µs)",
            "• Network: L6 Recv/Sent, RDMA Recv/Sent",
            "• Storage: OBS Upload/Download",
            "",
            "Modes:",
            "• Client Mode: Shows client hosts with aggregated metrics",
            "• Backend Mode: Shows backend hosts (Frontend/Compute/Drives) separately",
            "• Drill-down: Shows all process types for a selected backend host",
            "",
            "Backend Role Filtering (in drill-down):",
            "1 - Toggle DRIVES processes on/off",
            "2 - Toggle COMPUTE processes on/off", 
            "3 - Toggle FRONTEND processes on/off",
            "",
            "Press any key to continue..."
        ]
        
        # Create help rows
        help_rows = []
        for line in help_text:
            if line.startswith("WEKA Performance Monitor"):
                help_rows.append(urwid.AttrMap(urwid.Text(line, align='center'), 'help_header'))
            elif line.strip() == "":
                help_rows.append(urwid.Text(""))
            else:
                help_rows.append(urwid.AttrMap(urwid.Text(line), 'help_text'))
                
        # Update table body with help
        self.table.body = urwid.SimpleListWalker(help_rows)
        
        # Update footer
        self.footer_text = "Press any key to return to main view"
        self.footer.set_text(self.footer_text)
            
    def update_node_details_display(self):
        """Update the node details display"""
        if not self.node_details:
            # Show error message when no node details available
            rows = []
            rows.append(urwid.AttrMap(urwid.Text("No node details available for this host", align='center'), 'error'))
            rows.append(urwid.AttrMap(urwid.Text("Press Escape to return to main view", align='center'), 'help_text'))
            self.table.body = urwid.SimpleListWalker(rows)
            return
            
        # Create table rows
        rows = []
        
        # Header row - use same metric columns as main view (no hostname here)
        header_cells = ['Node']
        for col in self.metric_columns:
            header_cells.append(col)
                
        # Build header with fixed Node column, Role column, and weighted metric columns
        header_columns = []
        header_columns.append(('fixed', 6, urwid.AttrMap(urwid.Text('Node', align='left'), 'table_header')))
        header_columns.append(('fixed', 10, urwid.AttrMap(urwid.Text('Role', align='left'), 'table_header')))
        for col in self.metric_columns:
            header_columns.append(('weight', 0.1, urwid.AttrMap(urwid.Text(col, align='left'), 'table_header')))
        rows.append(urwid.Columns(header_columns))
        
        # Add totals/averages row directly under header (ALWAYS FIRST)
        totals = self.calculate_node_totals()
        if totals:
            total_cells = ['TOTALS/AVG']
            # Follow the same order as metric_columns (which matches the header)
            for col in self.metric_columns:
                # Determine the correct label based on metric type
                if col == 'CPU%':
                    label = "(max)"
                elif col in ['Read Latency(µs)', 'Write Latency(µs)']:
                    label = "(avg)"
                else:
                    label = "(total)"
                
                value = totals.get(col, 0)
                if isinstance(value, (int, float)):
                    if col == 'CPU%':
                        display_value = f"{value:.2f} {label}"
                    elif col in ['Read Latency(µs)', 'Write Latency(µs)']:
                        display_value = f"{value:.2f} {label}"
                    elif col in ['Ops/s', 'Reads/s', 'Writes/s']:
                        display_value = f"{self.format_ops(value)} {label}"
                    elif col in ['L6 Recv', 'L6 Sent', 'OBS Upload', 'OBS Download', 'RDMA Recv', 'RDMA Sent']:
                        display_value = f"{self.format_throughput(value)} {label}"
                    else:
                        display_value = f"{value} {label}"
                else:
                    display_value = f"{value} {label}"
                total_cells.append(display_value)

            # Build totals row with empty Node column, empty Role column, and weighted metric columns
            total_columns = []
            total_columns.append(('fixed', 6, urwid.Text('', align='left')))
            total_columns.append(('fixed', 10, urwid.Text('', align='left')))
            for cell in total_cells[1:]:
                total_columns.append(('weight', 0.1, urwid.Text(cell, align='left')))
            total_row = urwid.Columns(total_columns)
            total_row = urwid.AttrMap(total_row, 'table_header')
            rows.append(total_row)

        # Data rows - ONLY show actual node data, never totals
        node_row_count = 0  # Counter for actual node rows (excluding totals)
        
        # Apply role filters for backend mode
        filtered_node_details = self.node_details
        if self.current_mode == 'backend' and self.current_view == 'node_details':
            filtered_node_details = [node for node in self.node_details if 'node' in node and node.get('role') in self.role_filters and self.role_filters[node.get('role')]]
        
        for node_data in filtered_node_details:
            # Skip any data that doesn't have a 'node' field (these are totals)
            if 'node' not in node_data:
                continue
                
            node_row_count += 1  # Increment counter for actual node rows
            row_cells = []
            
            # Add node
            row_cells.append(str(node_data.get('node', 'N/A')))
            
            # Add role
            row_cells.append(str(node_data.get('role', 'N/A')))
            
            # Add metric values - use same order as metric_columns
            for col in self.metric_columns:
                value = node_data.get(col, 'N/A')
                
                if isinstance(value, (int, float)):
                    if col in ['CPU%', 'Read Latency(µs)', 'Write Latency(µs)']:
                        display_value = f"{value:.2f}"
                    elif col in ['Ops/s', 'Reads/s', 'Writes/s']:
                        display_value = self.format_ops(value)
                    elif col in ['L6 Recv', 'L6 Sent', 'OBS Upload', 'OBS Download', 'RDMA Recv', 'RDMA Sent']:
                        display_value = self.format_throughput(value)
                    else:
                        display_value = f"{value}"
                else:
                    display_value = str(value)
                    
                row_cells.append(display_value)
                    
            # Build row with fixed Node column, fixed Role column, and weighted metric columns
            row_columns = []
            row_columns.append(('fixed', 6, urwid.Text(row_cells[0], align='left')))  # Node
            row_columns.append(('fixed', 10, urwid.Text(row_cells[1], align='left')))  # Role
            for cell in row_cells[2:]:  # Skip node and role, start with metrics
                row_columns.append(('weight', 0.1, urwid.Text(cell, align='left')))
            row = urwid.Columns(row_columns)
            
            # Apply styling: selection takes priority, then banded rows
            if node_row_count - 1 == self.selected_row:
                # Selected row gets priority styling
                row = urwid.AttrMap(row, 'selected')
            else:
                # Non-selected rows get banded styling
                if node_row_count % 2 == 0:
                    row = urwid.AttrMap(row, 'table_row_alt')
                else:
                    row = urwid.AttrMap(row, 'table_row')
                
            rows.append(row)
            
        # Update table
        self.table.body = urwid.SimpleListWalker(rows)
        
        # Update footer using the centralized footer logic
        self.update_footer()

    def update_footer(self):
        """Update the footer text"""
        # Don't update footer if we're in row selection mode - preserve the selection input
        if self.row_selection_mode:
            return
            
        if self.current_view == 'node_details' and self.selected_host:
            # Drill-down view: show base hostname (without role) on left, help/quit on right
            if self.current_mode == 'backend' and '-' in self.selected_host:
                # Extract base hostname for backend mode
                parts = self.selected_host.split('-')
                if len(parts) >= 2:
                    base_hostname = '-'.join(parts[:-1])
                else:
                    base_hostname = self.selected_host
            else:
                base_hostname = self.selected_host
            # Add filter status for backend mode
            filter_status = ""
            if self.current_mode == 'backend':
                active_filters = [role for role, active in self.role_filters.items() if active]
                filter_status = f" | Filters: {', '.join(active_filters)} | Press 1/2/3 to toggle roles"
            
            left_text = f"Host: {base_hostname} | Refresh: {self.refresh_rate}s{filter_status}"
            right_text = "Press 'h' for help, 'q' to quit"
            # Pad the left text to push right text to the right
            padding = max(0, 80 - len(left_text) - len(right_text))
            self.footer_text = f"{left_text}{' ' * padding}{right_text}"
        else:
            # Main view: show info on left, help/quit on right
            left_text = f"Refresh: {self.refresh_rate}s | Columns: {len(self.visible_columns)} | Use 'a' to add, 'r' to remove | 's' for sort (:s+3, :s+CPU%, etc.)"
            right_text = "Press 'h' for help, 'q' to quit"
            # Pad the left text to push right text to the right
            padding = max(0, 80 - len(left_text) - len(right_text))
            self.footer_text = f"{left_text}{' ' * padding}{right_text}"
            
        # Update the footer widget directly
        self.footer.set_text(self.footer_text)

    def update_footer_with_selection_input(self):
        """Update footer with row selection input"""
        if self.current_view == 'node_details' and self.selected_host:
            # Drill-down view with row selection: show base hostname (without role) and : prompt on left, help/quit on right
            if self.current_mode == 'backend' and '-' in self.selected_host:
                # Extract base hostname for backend mode
                parts = self.selected_host.split('-')
                if len(parts) >= 2:
                    base_hostname = '-'.join(parts[:-1])
                else:
                    base_hostname = self.selected_host
            else:
                base_hostname = self.selected_host
            # Add filter status for backend mode
            filter_status = ""
            if self.current_mode == 'backend':
                active_filters = [role for role, active in self.role_filters.items() if active]
                filter_status = f" | Filters: {', '.join(active_filters)} | Press 1/2/3 to toggle roles"
            
            if self.row_selection_input.startswith('s'):
                left_text = f"Host: {base_hostname} | Refresh: {self.refresh_rate}s | : {self.row_selection_input} | Type +3 or -3, Enter to sort, Esc to cancel{filter_status}"
            else:
                left_text = f"Host: {base_hostname} | Refresh: {self.refresh_rate}s | : {self.row_selection_input} | Press Enter to select, Esc to cancel{filter_status}"
            right_text = "Press 'h' for help, 'q' to quit"
            # Pad the left text to push right text to the right
            padding = max(0, 80 - len(left_text) - len(right_text))
            footer_text = f"{left_text}{' ' * padding}{right_text}"
        else:
            # Main view with row selection: show info and : prompt on left, help/quit on right
            if self.row_selection_input.startswith('s'):
                left_text = f"Refresh: {self.refresh_rate}s | Columns: {len(self.visible_columns)} | : {self.row_selection_input} | Type +3 or -3, Enter to sort, Esc to cancel"
            else:
                left_text = f"Refresh: {self.refresh_rate}s | Columns: {len(self.visible_columns)} | : {self.row_selection_input} | Press Enter to select, Esc to cancel"
            right_text = " Press 'h' for help, 'q' to quit"
            # Pad the left text to push right text to the right
            padding = max(0, 80 - len(left_text) - len(right_text))
            footer_text = f"{left_text}{' ' * padding}{right_text}"
            
        # Update the footer widget directly
        self.footer.set_text(footer_text)



    def handle_input(self, key):
        """Handle keyboard input"""
        # Convert key to string for easier handling
        if isinstance(key, tuple):
            key = key[0] if key else ''
        key_str = str(key)
        

        
        if key_str == 'q':
            raise urwid.ExitMainLoop()
            
        elif key_str == 'h':
            if not self.showing_help:
                self.show_help()
            else:
                # If help is already showing, dismiss it
                self.return_from_help()
        elif key_str == 'm':
            # Switch between client and backend modes
            self.current_mode = 'backend' if self.current_mode == 'client' else 'client'
            self.update_mode_display()
            
            # Only clear data and switch views if we're in the main view
            # If user is in drill-down, just switch modes but keep their current view
            if self.current_view == 'main':
                # Clear current data and refresh
                self.hosts = {}
                self.backend_hosts = {}
                self.update_data()
            else:
                # We're in drill-down view, just clear the node details for the new mode
                # but keep the user in drill-down view
                self.node_details = {}
                self.selected_host = None
        elif key_str == 's':
            # Enter sort command mode (same as row selection mode)
            self.row_selection_mode = True
            self.row_selection_input = "s"
            self.update_footer_with_selection_input()
        elif key_str == '+' and not self.row_selection_mode:
            self.refresh_rate = min(self.refresh_rate + 0.5, 10.0)
            # Update footer immediately
            self.update_footer()
        elif key_str == '-' and not self.row_selection_mode:
            self.refresh_rate = max(self.refresh_rate - 0.5, 0.5)
            # Update footer immediately
            self.update_footer()
        elif key_str in ('+', '-') and self.row_selection_mode and self.row_selection_input.startswith('s'):
            # Add + or - to sort command input
            self.row_selection_input += key_str
            self.update_footer_with_selection_input()
        elif key_str == 'a':
            # Add column
            self.add_column()
        elif key_str == 'r':
            # Remove column
            self.remove_column()
        elif key_str in ('1', '2', '3') and self.current_view == 'node_details' and self.current_mode == 'backend':
            # Role filter shortcuts for backend drill-down
            # 1 = DRIVES, 2 = COMPUTE, 3 = FRONTEND
            role_map = {'1': 'DRIVES', '2': 'COMPUTE', '3': 'FRONTEND'}
            role = role_map.get(key_str)
            if role:
                self.role_filters[role] = not self.role_filters[role]  # Toggle filter
                # Refresh the display to show filtered results
                self.update_node_details_display()
                # Update footer to show current filter status
                self.update_footer()
        elif key_str == ':':
            # Enter row selection mode
            self.row_selection_mode = True
            self.row_selection_input = ""
            self.update_footer_with_selection_input()
        elif key_str == 'enter':
            if self.row_selection_mode:
                # Process row selection
                self.process_row_selection()
            elif self.current_view == 'main' and self.selected_row < len(self.hosts):
                # Drill down to selected host
                self.drill_down_to_host()
            elif self.current_view == 'node_details':
                # Return to main view
                self.return_to_main_view()
        elif key_str in ('up', 'down'):
            if self.current_view == 'main':
                # Navigate in main view
                if key_str == 'up':
                    self.selected_row = max(0, self.selected_row - 1)
                else:
                    self.selected_row = min(len(self.hosts) - 1, self.selected_row + 1)
                self.update_display()
            elif self.current_view == 'node_details':
                # Navigate in node details view
                if key_str == 'up':
                    self.selected_row = max(0, self.selected_row - 1)
                else:
                    self.selected_row = min(len(self.node_details) - 1, self.selected_row + 1)
                self.update_node_details_display()
        elif key_str.isdigit() and self.row_selection_mode:
            # Add digit to row selection input
            self.row_selection_input += key_str
            self.update_footer_with_selection_input()
        elif key_str == 'd' and self.row_selection_mode and self.row_selection_input:
            # Quick drill down - check this BEFORE general letter input
            try:
                row_num = int(self.row_selection_input) - 1
                if 0 <= row_num < len(self.hosts):
                    self.selected_row = row_num
                    self.row_selection_mode = False
                    self.row_selection_input = ""
                    self.drill_down_to_host()
            except ValueError:
                pass
        elif key_str == 'backspace' and self.row_selection_mode:
            # Handle the literal "backspace" string that urwid is sending - MUST be early in the chain
            if self.row_selection_input:
                self.row_selection_input = self.row_selection_input[:-1]
                self.update_footer_with_selection_input()
            return True  # Explicitly indicate we handled this key
        elif key_str in ('escape', 'esc'):
            if self.row_selection_mode:
                # Cancel row selection mode completely (takes priority)
                self.row_selection_mode = False
                self.row_selection_input = ""
                self.update_footer()
            elif self.current_view == 'node_details':
                # Return to main view (only if not in row selection mode)
                self.return_to_main_view()
        elif key_str.isalpha() and self.row_selection_mode:
            # Add letters to row selection input (for column names in sorting)
            self.row_selection_input += key_str
            self.update_footer_with_selection_input()
        elif key_str in ('%', '/', '(', ')', 'µ', 's') and self.row_selection_mode:
            # Add special characters commonly used in column names
            self.row_selection_input += key_str
            self.update_footer_with_selection_input()
        elif key_str.isdigit() and not self.row_selection_mode and not self.showing_help:
            # Handle column cycling with number keys 1-9
            try:
                column_index = int(key_str)
                if 1 <= column_index <= 9:  # Only handle keys 1-9
                    self.cycle_column(column_index)
            except ValueError:
                pass


        # Handle help screen dismissal with any key (must be early to catch all keys)
        elif self.showing_help:
            # Return from help with any key
            self.return_from_help()


            
        return True  # Indicate we handled the input
            
    def cycle_column(self, column_index):
        """Cycle through available metrics for a specific column"""
        # Key 1 → First metric column, Key 2 → Second metric column, etc.
        # The table structure is: [ID, spacer, Hostname, metric1, metric2, metric3, ...]
        # So Key 1 should change metric1, Key 2 should change metric2, etc.
        
        # Convert key to metric column index (0-based)
        metric_index = column_index - 1  # Key 1 → index 0, Key 2 → index 1, etc.
        
        # Check if this metric index exists
        if metric_index >= len(self.metric_columns):
            return
            
        # Get available metrics (exclude ID and Hostname)
        available_metrics = [col for col in self.available_metrics.keys() if col not in ['Hostname']]
        
        if not available_metrics:
            return
            
        # Find current metric in this column
        current_metric = self.metric_columns[metric_index]
        
        # Find next unique metric that's not already used in other columns
        used_metrics = set(self.metric_columns)
        current_index = available_metrics.index(current_metric) if current_metric in available_metrics else -1
        
        # Try to find the next available unique metric
        next_metric = None
        for i in range(len(available_metrics)):
            next_index = (current_index + i + 1) % len(available_metrics)
            candidate = available_metrics[next_index]
            if candidate not in used_metrics or candidate == current_metric:
                next_metric = candidate
                break
        
        # If no unique metric found, keep current one
        if next_metric is None:
            return
            
        # Update the metric column
        self.metric_columns[metric_index] = next_metric
        
        # Also update visible_columns to keep them in sync
        visible_index = metric_index + 1  # +1 because visible_columns[0] is Hostname
        if visible_index < len(self.visible_columns):
            self.visible_columns[visible_index] = next_metric
        # Update display based on current view
        if self.current_view == 'node_details':
            self.update_node_details_display()
        else:
            self.update_display()
        
    def get_unique_initial_metrics(self):
        """Get a list of unique initial metrics for the columns"""
        # Start with the default metrics but ensure they're unique
        default_metrics = ['CPU%', 'Ops/s', 'Reads/s', 'Writes/s', 'Read Latency(µs)', 'Write Latency(µs)']
        
        # Get all available metrics
        available_metrics = [col for col in self.available_metrics.keys() if col not in ['Hostname']]
        
        # Build unique list, starting with defaults, then adding others
        unique_metrics = []
        used_metrics = set()
        
        # Add default metrics first (if available)
        for metric in default_metrics:
            if metric in available_metrics and metric not in used_metrics:
                unique_metrics.append(metric)
                used_metrics.add(metric)
        
        # Add remaining available metrics
        for metric in available_metrics:
            if metric not in used_metrics:
                unique_metrics.append(metric)
                used_metrics.add(metric)
        
        # Limit to reasonable number of columns (6-8)
        return unique_metrics[:6]
        
    def add_column(self):
        """Add a new metric column"""
        # Get available metrics that aren't already visible
        if self.current_view == 'main':
            visible_metrics = set(self.metric_columns)
            available_metrics = [col for col in self.available_metrics.keys() if col not in visible_metrics and col != 'Hostname']
            
            if available_metrics:
                # Add the first available metric to the right of hostname
                self.metric_columns.append(available_metrics[0])
                # Sync visible_columns for rendering
                self.visible_columns = ['Hostname'] + self.metric_columns
                # Update main view stored metrics
                self.main_view_metric_columns = self.metric_columns.copy()
                self.update_display()
        else:
            # In drill-down view, work with current visible_columns
            visible_metrics = set(self.metric_columns)
            available_metrics = [col for col in self.available_metrics.keys() if col not in visible_metrics and col != 'Hostname']
            
            if available_metrics:
                # Add the first available metric
                self.metric_columns.append(available_metrics[0])
                self.visible_columns = ['Hostname'] + self.metric_columns
                self.update_node_details_display()
            
    def remove_column(self):
        """Remove the last metric column"""
        if len(self.metric_columns) > 0:
            # Remove the last metric column
            self.metric_columns.pop()
            self.visible_columns = ['Hostname'] + self.metric_columns
            # Update the appropriate display based on current view
            if self.current_view == 'main':
                # Update main view stored metrics
                self.main_view_metric_columns = self.metric_columns.copy()
                self.update_display()
            else:
                self.update_node_details_display()
            
    def process_row_selection(self):
        """Process row selection input"""
        if not self.row_selection_input:
            return
            
        # Check if this is a sort command
        if self.row_selection_input.startswith('s'):
            self.process_sort_command()
            return
            
        try:
            row_num = int(self.row_selection_input) - 1
            if 0 <= row_num < len(self.hosts):
                self.selected_row = row_num
                self.row_selection_mode = False
                self.row_selection_input = ""
                if self.current_view == 'main':
                    self.update_display()
                else:
                    self.update_node_details_display()
            else:
                # Invalid row number
                self.row_selection_mode = False
                self.row_selection_input = ""
                self.update_footer()
        except ValueError:
            # Invalid input
            self.row_selection_mode = False
            self.row_selection_input = ""
            self.update_footer()
            
    def process_sort_command(self):
        """Process sort command in format :s+3, :s-3, :s+CPU%, :s-Ops/s, etc."""
        try:
            # Parse sort command: s+3, s-3, s+CPU%, s-Ops/s, etc.
            if len(self.row_selection_input) < 2:
                # Invalid format
                self.row_selection_mode = False
                self.row_selection_input = ""
                self.update_footer()
                return
                
            sort_direction = self.row_selection_input[1]  # + or -
            if sort_direction not in ['+', '-']:
                # Invalid direction
                self.row_selection_mode = False
                self.row_selection_input = ""
                self.update_footer()
                return
                
            # Get the rest of the input (column number or name)
            column_input = self.row_selection_input[2:]
            
            # Try to parse as column number first
            try:
                column_num = int(column_input)
                if 1 <= column_num <= len(self.metric_columns):
                    # Valid column number
                    metric_index = column_num - 1
                    metric_name = self.metric_columns[metric_index]
                else:
                    # Column number out of range
                    self.row_selection_mode = False
                    self.row_selection_input = ""
                    self.update_footer()
                    return
            except ValueError:
                # Not a number, try to find by column name
                metric_name = column_input
                if metric_name not in self.metric_columns:
                    # Invalid column name
                    self.row_selection_mode = False
                    self.row_selection_input = ""
                    self.update_footer()
                    return
            
            # Sort the data
            self.sort_data(metric_name, sort_direction == '+')
            
            # Exit row selection mode
            self.row_selection_mode = False
            self.row_selection_input = ""
            
            # Update display based on current view
            if self.current_view == 'main':
                self.update_display()
            else:
                self.update_node_details_display()
                
        except (ValueError, IndexError):
            # Invalid format
            self.row_selection_mode = False
            self.row_selection_input = ""
            self.update_footer()
            
    def sort_data(self, metric_name, ascending=True):
        """Sort data by the specified metric"""
        # Store the sort state for preservation across data updates
        self.sort_column = metric_name
        self.sort_reverse = not ascending
        

        
        if self.current_view == 'main':
            # Sort hosts
            if self.hosts:
                # Convert to list for sorting
                host_list = list(self.hosts.items())
                
                # Sort by the specified metric
                host_list.sort(key=lambda x: x[1].get(metric_name, 0), reverse=not ascending)
                
                # Rebuild hosts dict in sorted order
                self.hosts = dict(host_list)
                
                # Reset selected row to top
                self.selected_row = 0
        else:
            # Sort node details
            if self.node_details:
                # Sort by the specified metric (EXCLUDE totals from sorting)
                node_data_only = [node for node in self.node_details if 'node' in node]
                node_data_only.sort(key=lambda x: x.get(metric_name, 0), reverse=not ascending)
                # Reconstruct node_details with totals first, then sorted nodes
                totals_row = [node for node in self.node_details if 'node' not in node]
                self.node_details = totals_row + node_data_only
                
                # Reset selected row to top
                self.selected_row = 0
            else:
                pass
            
    def drill_down_to_host(self):
        """Drill down to show node details for selected host"""
        if self.selected_row >= len(self.hosts):
            return
            
        # Get the selected host
        host_list = list(self.hosts.items())
        hostname = host_list[self.selected_row][0]
        
        # Note: Debug output removed for cleaner UI
        
        # Store the selected host
        self.selected_host = hostname
        
        # Save current main view columns and switch to drill-down columns
        self.main_view_metric_columns = self.metric_columns.copy()
        
        # Get node details based on mode
        if self.current_mode == 'backend':
            # For backend mode, extract base hostname and specific role
            if '-' in hostname:
                # The hostname format is: "hcsf1-01.entstorage-DRIVES"
                # We need to extract both "hcsf1-01.entstorage" and "DRIVES"
                parts = hostname.split('-')
                if len(parts) >= 2:
                    base_hostname = '-'.join(parts[:-1])  # Everything except the role part
                    selected_role = parts[-1]  # The specific role (DRIVES, COMPUTE, FRONTEND)
                else:
                    base_hostname = hostname
                    selected_role = None
                
                # Get all nodes for this base hostname (show all process types)
                self.node_details = self.get_backend_host_node_details(base_hostname)
            else:
                # Fallback to regular node details
                self.node_details = self.get_host_node_details(hostname)
        else:
            # Client mode - use existing logic
            self.node_details = self.get_host_node_details(hostname)
        
        # Switch to node details view
        self.current_view = 'node_details'
        
        # Update display
        self.update_node_details_display()
        
    def return_to_main_view(self):
        """Return to the main view from node details"""
        self.current_view = 'main'
        self.selected_host = None
        self.node_details = {}
        self.selected_row = 0
        
        # Restore main view column configuration
        self.metric_columns = self.main_view_metric_columns.copy()
        self.visible_columns = ['Hostname'] + self.metric_columns
        
        # Update display
        self.update_display()
        
    def show_help(self):
        """Show the help screen"""
        self.showing_help = True
        self.show_help_screen()
        
    def return_from_help(self):
        """Return from help screen"""
        self.showing_help = False
        if self.current_view == 'main':
            self.update_display()
        else:
            self.update_node_details_display()
            
    def format_ops(self, ops):
        """Format operations per second in human readable format"""
        if ops >= 1000000:
            return f"{ops/1000000:.2f}Mops"
        elif ops >= 1000:
            return f"{ops/1000:.2f}Kops"
        else:
            return f"{ops:.2f}"
            
    def format_throughput(self, bytes_per_sec):
        """Format throughput in human readable format"""
        if bytes_per_sec >= 1099511627776:  # 1TB
            return f"{bytes_per_sec/1099511627776:.2f}TB/s"
        elif bytes_per_sec >= 1073741824:  # 1GB
            return f"{bytes_per_sec/1073741824:.2f}GB/s"
        elif bytes_per_sec >= 1048576:  # 1MB
            return f"{bytes_per_sec/1048576:.2f}MB/s"
        elif bytes_per_sec >= 1024:  # 1KB
            return f"{bytes_per_sec/1024:.2f}KB/s"
        else:
            return f"{bytes_per_sec:.2f}B/s"
            
    def format_capacity(self, bytes_val):
        """Format capacity in human readable format"""
        if bytes_val >= 1099511627776:  # 1TB
            return f"{bytes_val/1099511627776:.2f}TB"
        elif bytes_val >= 1073741824:  # 1GB
            return f"{bytes_val/1073741824:.2f}GB"
        else:
            return f"{bytes_val:.2f}B"

def main():
    """Main entry point"""
    try:
        monitor = WekaMonitor()
        monitor.run()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
