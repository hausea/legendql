"""
Utilities for interacting with the Pure Relation REPL.
"""
import os
import csv
import tempfile
import sys
import subprocess
import time
import json
import re
import threading
import queue

repl_process = None
repl_output_queue = queue.Queue()
repl_ready = threading.Event()

def start_repl():
    """Start the REPL and keep it running."""
    global repl_process

    if repl_process is not None:
        print("REPL is already running.")
        return

    cmd = "java -jar ../../legend-engine-repl-relational-4.74.1-SNAPSHOT.jar"
    print(f"Starting REPL with command: {cmd}")

    repl_process = subprocess.Popen(
        cmd,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )

    def read_output():
        while repl_process.poll() is None:
            line = repl_process.stdout.readline()
            if line:
                repl_output_queue.put(line)
                print(f"REPL output: {line.strip()}")
                if "REPL ready" in line or "Press 'Enter'" in line or "legend" in line.lower():
                    repl_ready.set()

    output_thread = threading.Thread(target=read_output, daemon=True)
    output_thread.start()

    print("Waiting for REPL to be ready...")
    wait_start = time.time()
    max_wait = 30  # seconds

    if not repl_ready.wait(timeout=max_wait):
        print("WARNING: REPL startup may not be complete yet. Sending test command...")
        try:
            repl_process.stdin.write("\n")
            repl_process.stdin.flush()
            time.sleep(5)
            if not repl_ready.is_set():
                print("WARNING: REPL still not ready after additional wait time.")
            else:
                print("REPL is now ready to accept commands.")
        except Exception as e:
            print(f"Error checking REPL readiness: {e}")
    else:
        print("REPL is ready to accept commands.")

    try:
        print("Sending test command to verify REPL is responsive...")
        repl_process.stdin.write("help\n")
        repl_process.stdin.flush()

        verification_timeout = time.time() + 10
        while time.time() < verification_timeout:
            try:
                output = repl_output_queue.get(timeout=0.5)
                if output and ("Available commands" in output or "help" in output):
                    print("REPL verified as responsive!")
                    break
            except queue.Empty:
                continue

        time.sleep(2)
    except Exception as e:
        print(f"Error sending verification command: {e}")

    return repl_process


def send_to_repl(command, timeout=10):
    """Send a command to the running REPL process."""
    global repl_process, repl_ready

    if repl_process is None or repl_process.poll() is not None:
        print("Starting REPL...")
        start_repl()

    if not repl_ready.is_set():
        print("Waiting for REPL to be ready before sending command...")
        if not repl_ready.wait(timeout=30):
            print("WARNING: REPL may not be fully ready, but attempting to send command anyway.")

    print(f"Sending to REPL: {command}")

    try:
        try:
            while True:
                repl_output_queue.get_nowait()
        except queue.Empty:
            pass

        repl_process.stdin.write(command + "\n")
        repl_process.stdin.flush()

        print("Waiting for REPL response...")
        wait_start = time.time()
        max_wait = 20  # Increased timeout for complex queries

        time.sleep(1)

        output = []
        consecutive_empty_count = 0
        max_consecutive_empty = 3  # Wait for this many consecutive empty reads before concluding

        if command.startswith("#>"):
            max_wait = 60  # Much longer timeout for Pure expressions with debug output
            max_consecutive_empty = 8  # More patience for Pure expressions with verbose output

        while time.time() - wait_start < max_wait:
            try:
                line = repl_output_queue.get(timeout=0.5)
                output.append(line)
                print(f"Received: {line.strip()}")
                wait_start = time.time()  # Reset wait timer when we get output
                consecutive_empty_count = 0  # Reset empty counter
            except queue.Empty:
                consecutive_empty_count += 1
                if output and consecutive_empty_count >= max_consecutive_empty:
                    print(f"No more output after {consecutive_empty_count} consecutive empty reads")
                    break
                continue

        result = "".join(output)
        if not result:
            print("No output received from REPL within timeout period.")
            return "Command sent to REPL (no output within timeout period)"

        print(f"Total output length: {len(result)} characters, {len(output)} lines")
        return result

    except Exception as e:
        print(f"Error sending command to REPL: {e}")
        return f"Error: {str(e)}"


def load_csv_to_repl(csv_path, connection_name, table_name):
    """
    Load a CSV file into the REPL.

    Args:
        csv_path: Path to the CSV file
        connection_name: Name of the connection to use
        table_name: Name of the table to create

    Returns:
        dict: The parsed JSON response from the REPL
    """
    load_cmd = f"load {csv_path} {connection_name} {table_name}"
    return send_to_repl(load_cmd)


def execute_pure_query(pure_query):
    """
    Execute a Pure Relation query in the REPL.

    Args:
        pure_query: The Pure Relation query to execute

    Returns:
        dict: The parsed JSON response from the REPL
    """
    return send_to_repl(pure_query)


def is_repl_running():
    """
    Check if the Pure Relation REPL is running.

    Returns:
        bool: True if the REPL is running, False otherwise
    """
    if repl_process is None:
        start_repl()

    return repl_process != None
