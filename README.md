Here's the updated function that gets dates from the start date until yesterday:

```python
from datetime import datetime, timedelta

def generate_daily_dates(start_date_str):
    """
    Generate a list of dates in 'yyyymmdd' format from start date until yesterday.
    
    Args:
        start_date_str (str): Starting date in 'yyyymmdd' format
        
    Returns:
        list: List of date strings in 'yyyymmdd' format
    """
    # Parse the start date string into a datetime object
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    
    # Get yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    
    # Make sure the yesterday date has time set to midnight for proper comparison
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Check if start date is in the future or today
    if start_date >= datetime.now().replace(hour=0, minute=0, second=0, microsecond=0):
        return []  # Return empty list if start date is in the future or today
    
    # Generate the list of dates
    date_list = []
    current_date = start_date
    
    while current_date <= yesterday:
        date_str = current_date.strftime('%Y%m%d')
        date_list.append(date_str)
        current_date += timedelta(days=1)
    
    return date_list

# Example usage:
if __name__ == "__main__":
    # Generate dates from January 1, 2025 until yesterday
    dates = generate_daily_dates('20250101')
    print(f"Generated {len(dates)} dates from 20250101 until yesterday")
    if dates:
        print(f"First date: {dates[0]}, Last date: {dates[-1]}")
```

This updated function:
1. Takes a start date as a string in 'yyyymmdd' format
2. Calculates yesterday's date (current date minus one day)
3. Generates dates incrementally starting from the provided date until yesterday
4. Returns an empty list if the start date is in the future or today
5. Returns all dates as a list in 'yyyymmdd' format

The function handles edge cases such as:
- Start date in the future
- Start date is today
- Start date is after yesterday