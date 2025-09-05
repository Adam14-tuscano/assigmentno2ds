#!/usr/bin/env python3
"""
Setup and Demo Script for Distributed Sales Processing System
============================================================

This script helps set up and demonstrate the distributed sales processing system.
It includes utilities for creating sample data, running demos, and managing the system.
"""

import os
import subprocess
import sys
import time
import threading
import pandas as pd
import numpy as np
from pathlib import Path

def create_large_sample_dataset(filename='sales_data_5m.csv', num_rows=5000000):
    """Create a large sample sales dataset similar to Kaggle format"""
    print(f"Creating sample dataset with {num_rows:,} rows...")
    
    # Set random seed for reproducibility
    np.random.seed(42)
    
    # Generate realistic sales data
    batch_size = 100000  # Process in batches to manage memory
    
    # Initialize CSV file with headers
    columns = ['TransactionID', 'OrderDate', 'ProductID', 'ProductName', 
               'Category', 'Price', 'Quantity', 'CustomerID', 'Region']
    
    pd.DataFrame(columns=columns).to_csv(filename, index=False)
    
    # Categories and regions for realistic data
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 
                 'Toys', 'Health', 'Automotive', 'Food', 'Beauty']
    regions = ['North', 'South', 'East', 'West', 'Central']
    product_names = [f'Product_{i}' for i in range(1, 1001)]
    
    print("Generating data in batches...")
    for batch_start in range(0, num_rows, batch_size):
        batch_end = min(batch_start + batch_size, num_rows)
        batch_rows = batch_end - batch_start
        
        # Fixed date generation - avoid division by zero
        # Calculate appropriate frequency to spread dates over time
        total_days = 365 * 4  # 4 years worth of data
        hours_per_batch = max(1, total_days * 24 // batch_rows)
        
        # Generate batch data
        batch_data = {
            'TransactionID': range(batch_start + 1, batch_end + 1),
            'OrderDate': pd.date_range('2020-01-01', periods=batch_rows, 
                                     freq=f'{hours_per_batch}H'),  # Use hours instead of minutes
            'ProductID': np.random.randint(1000, 9999, batch_rows),
            'ProductName': np.random.choice(product_names, batch_rows),
            'Category': np.random.choice(categories, batch_rows),
            'Price': np.round(np.random.lognormal(mean=3, sigma=1, size=batch_rows), 2),
            'Quantity': np.random.randint(1, 10, batch_rows),
            'CustomerID': np.random.randint(10000, 99999, batch_rows),
            'Region': np.random.choice(regions, batch_rows)
        }
        
        batch_df = pd.DataFrame(batch_data)
        
        # Append to CSV file
        batch_df.to_csv(filename, mode='a', header=False, index=False)
        
        if (batch_start // batch_size + 1) % 10 == 0:
            print(f"  Generated {batch_end:,} rows...")
    
    print(f"Sample dataset created: {filename}")
    print(f"File size: {os.path.getsize(filename) / (1024*1024):.1f} MB")

def run_demo_with_multiple_workers(csv_file=None, num_workers=3):
    """Run a complete demo with multiple workers"""
    print("="*80)
    print("DISTRIBUTED SALES PROCESSING SYSTEM DEMO")
    print("="*80)
    
    if not csv_file:
        csv_file = 'sales_data_1m.csv'
        if not os.path.exists(csv_file):
            print(f"Creating sample dataset: {csv_file}")
            create_large_sample_dataset(csv_file, 1000000)  # 1M rows for demo
    
    print(f"Using dataset: {csv_file}")
    print(f"Number of workers: {num_workers}")
    
    # Start server in a separate process
    print("\n1. Starting server...")
    server_process = subprocess.Popen([
        sys.executable, 'distributed_sales_system.py', 'server', csv_file
    ])
    
    # Wait for server to start
    time.sleep(3)
    
    # Start multiple workers
    print(f"\n2. Starting {num_workers} workers...")
    worker_processes = []
    
    for i in range(num_workers):
        print(f"  Starting worker {i+1}...")
        worker_process = subprocess.Popen([
            sys.executable, 'distributed_sales_system.py', 'worker'
        ])
        worker_processes.append(worker_process)
        time.sleep(1)  # Stagger worker starts
    
    print(f"\n3. Processing data with {num_workers} workers...")
    print("   (Check the server output for detailed progress)")
    
    # Wait for server to complete
    try:
        server_process.wait()
        print("\n4. Processing completed!")
    except KeyboardInterrupt:
        print("\n4. Demo interrupted by user")
    
    # Cleanup
    print("\n5. Cleaning up processes...")
    for worker_process in worker_processes:
        worker_process.terminate()
    
    if server_process.poll() is None:
        server_process.terminate()

def create_batch_script():
    """Create batch/shell scripts for easy execution"""
    
    # Windows batch script
    batch_content = """@echo off
echo Distributed Sales Processing System
echo ===================================

echo.
echo Choose an option:
echo 1. Create sample dataset (1M rows)
echo 2. Create large dataset (5M rows)  
echo 3. Run server
echo 4. Run worker
echo 5. Run demo with 3 workers

set /p choice="Enter choice (1-5): "

if "%choice%"=="1" (
    python setup_and_demo.py create_small
) else if "%choice%"=="2" (
    python setup_and_demo.py create_large
) else if "%choice%"=="3" (
    python distributed_sales_system.py server
) else if "%choice%"=="4" (
    python distributed_sales_system.py worker  
) else if "%choice%"=="5" (
    python setup_and_demo.py demo
) else (
    echo Invalid choice
)

pause
"""
    
    with open('run_system.bat', 'w') as f:
        f.write(batch_content)
    
    # Unix shell script
    shell_content = """#!/bin/bash
echo "Distributed Sales Processing System"
echo "==================================="

echo ""
echo "Choose an option:"
echo "1. Create sample dataset (1M rows)"
echo "2. Create large dataset (5M rows)"
echo "3. Run server"
echo "4. Run worker"
echo "5. Run demo with 3 workers"

read -p "Enter choice (1-5): " choice

case $choice in
    1)
        python3 setup_and_demo.py create_small
        ;;
    2)
        python3 setup_and_demo.py create_large
        ;;
    3)
        python3 distributed_sales_system.py server
        ;;
    4)
        python3 distributed_sales_system.py worker
        ;;
    5)
        python3 setup_and_demo.py demo
        ;;
    *)
        echo "Invalid choice"
        ;;
esac
"""
    
    with open('run_system.sh', 'w') as f:
        f.write(shell_content)
    
    # Make shell script executable
    os.chmod('run_system.sh', 0o755)
    
    print("Created batch scripts:")
    print("  - run_system.bat (Windows)")
    print("  - run_system.sh (Unix/Linux/Mac)")

def check_requirements():
    """Check if required packages are installed"""
    required_packages = ['pandas', 'numpy']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✓ {package} is installed")
        except ImportError:
            print(f"✗ {package} is missing")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\nInstall missing packages with:")
        print(f"pip install {' '.join(missing_packages)}")
        return False
    
    print("\n✓ All required packages are installed")
    return True

def main():
    """Main entry point for setup script"""
    if len(sys.argv) < 2:
        print("Distributed Sales Processing System Setup")
        print("=" * 50)
        print("Available commands:")
        print("  create_small  - Create 1M row sample dataset")
        print("  create_large  - Create 5M row sample dataset")
        print("  demo         - Run complete demo with 3 workers")
        print("  check        - Check requirements")
        print("  scripts      - Create batch scripts")
        print("")
        print("Examples:")
        print("  python setup_and_demo.py create_small")
        print("  python setup_and_demo.py demo")
        return
    
    command = sys.argv[1].lower()
    
    if command == 'create_small':
        create_large_sample_dataset('sales_data_1m.csv', 1000000)
        
    elif command == 'create_large':
        create_large_sample_dataset('sales_data_5m.csv', 5000000)
        
    elif command == 'demo':
        csv_file = 'sales_data_1m.csv' if len(sys.argv) < 3 else sys.argv[2]
        run_demo_with_multiple_workers(csv_file)
        
    elif command == 'check':
        check_requirements()
        
    elif command == 'scripts':
        create_batch_script()
        
    else:
        print(f"Unknown command: {command}")

if __name__ == "__main__":
    main()