"""
Utilities for working with CSV data, particularly for balance history fallbacks.
"""

import os
import csv
import logging
from datetime import datetime
from typing import List, Dict, Optional, Union, Any

logger = logging.getLogger(__name__)

class CSVBalanceHistoryManager:
    """Helper class to manage balance history in CSV files."""
    
    def __init__(self, file_path: Optional[str] = None):
        """Initialize with optional custom path."""
        if file_path:
            self.file_path = file_path
        else:
            # Default to project root / balance_history.csv
            self.file_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "balance_history.csv"
            )
        
        logger.debug(f"CSV Balance History Manager initialized with path: {self.file_path}")
    
    def file_exists(self) -> bool:
        """Check if the CSV file exists."""
        return os.path.isfile(self.file_path)
    
    def read_history(self, 
                    start_time: Optional[datetime] = None, 
                    end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Read balance history from CSV file with optional date filtering.
        
        Args:
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            List of balance history entries
        """
        if not self.file_exists():
            logger.warning(f"CSV file not found: {self.file_path}")
            return []
        
        history = []
        
        try:
            with open(self.file_path, mode='r') as file:
                csv_reader = csv.DictReader(file)
                
                for row in csv_reader:
                    try:
                        # Parse timestamp
                        timestamp = datetime.fromisoformat(row['timestamp'])
                        
                        # Apply date filtering if specified
                        if start_time and timestamp < start_time:
                            continue
                            
                        if end_time and timestamp > end_time:
                            continue
                        
                        # Add to history list
                        history.append({
                            'date': row['timestamp'],
                            'balance': float(row['balance']),
                            'currency': row['currency']
                        })
                        
                    except (ValueError, KeyError) as e:
                        logger.warning(f"Error parsing CSV row: {str(e)}")
                        continue
            
            logger.info(f"Read {len(history)} entries from CSV file")
            return history
            
        except Exception as e:
            logger.error(f"Error reading CSV file: {str(e)}")
            return []
    
    def write_entry(self, timestamp: Union[datetime, str], 
                  balance: float, currency: str = "USDT") -> bool:
        """
        Write a new balance history entry to the CSV file.
        
        Args:
            timestamp: Entry timestamp (datetime object or ISO format string)
            balance: Balance amount
            currency: Currency code (default: USDT)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert datetime to string if needed
            if isinstance(timestamp, datetime):
                timestamp = timestamp.isoformat()
            
            # Check if file exists and create with header if not
            file_exists = self.file_exists()
            
            with open(self.file_path, mode='a', newline='') as file:
                writer = csv.writer(file)
                
                # Write header if new file
                if not file_exists:
                    writer.writerow(["timestamp", "balance", "currency"])
                
                # Write data row
                writer.writerow([timestamp, balance, currency])
            
            logger.debug(f"Added balance entry: {timestamp}, {balance} {currency}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing to CSV file: {str(e)}")
            return False
    
    def generate_synthetic_data(self, 
                             start_time: datetime,
                             end_time: datetime,
                             current_balance: float,
                             days_interval: int = 1) -> List[Dict[str, Any]]:
        """
        Generate synthetic balance history for testing or fallback.
        
        Args:
            start_time: Start date for synthetic data
            end_time: End date for synthetic data
            current_balance: Current balance to work backwards from
            days_interval: Number of days between data points
            
        Returns:
            List of synthetic balance history entries
        """
        import random
        
        # Calculate days range
        days_range = (end_time - start_time).days + 1
        
        # Generate daily balances
        balance = current_balance
        synthetic_data = []
        
        for day in range(0, days_range, days_interval):
            date = end_time - timedelta(days=day)
            
            # Random change between -1% and +1%
            change_pct = random.uniform(-0.01, 0.01)
            
            # For historical data, apply change and store the value
            balance = balance / (1 + change_pct)  # Work backwards
            
            synthetic_data.append({
                'date': date.isoformat(),
                'balance': round(balance, 2),
                'currency': 'USDT'
            })
        
        # Reverse to get chronological order
        synthetic_data.reverse()
        
        logger.info(f"Generated {len(synthetic_data)} synthetic data points")
        return synthetic_data