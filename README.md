Here's a Python function that fetches dates incrementally for every day in 'yyyymmdd' format, starting from a given date string:

```python
from datetime import datetime, timedelta

def generate_daily_dates(start_date_str, num_days=30):
    """
    Generate a list of dates in 'yyyymmdd' format, incrementing one day at a time.
    
    Args:
        start_date_str (str): Starting date in 'yyyymmdd' format
        num_days (int, optional): Number of days to generate. Defaults to 30.
        
    Returns:
        list: List of date strings in 'yyyymmdd' format
    """
    # Parse the start date string into a datetime object
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    
    # Generate the list of dates
    date_list = []
    for i in range(num_days):
        current_date = start_date + timedelta(days=i)
        date_str = current_date.strftime('%Y%m%d')
        date_list.append(date_str)
    
    return date_list

# Example usage:
if __name__ == "__main__":
    # Generate 10 days starting from January 1, 2025
    dates = generate_daily_dates('20250101', 10)
    print(dates)
    # Output: ['20250101', '20250102', '20250103', '20250104', '20250105', 
    #          '20250106', '20250107', '20250108', '20250109', '20250110']
```

This function:
1. Takes a start date as a string in 'yyyymmdd' format
2. Optionally takes the number of days to generate (defaults to 30)
3. Converts the start date string to a datetime object
4. Creates a list of date strings by incrementing one day at a time
5. Returns the list of date strings, each in 'yyyymmdd' format

You can modify this function to suit your specific needs, such as returning a generator instead of a list (for memory efficiency) or using different date formats.