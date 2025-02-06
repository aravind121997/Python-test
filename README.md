from datetime import datetime

def convert_pipeline_expression(trigger_time):
    # Format the date part
    formatted_date = trigger_time.strftime('%Y/%m/%d')
    
    # Calculate ticks (Microsoft ticks are measured from 1601-01-01)
    # We need to convert to Unix epoch (1970-01-01) equivalent
    epoch = datetime(1970, 1, 1)
    ticks_since_epoch = int((trigger_time - epoch).total_seconds() * 10000000)
    
    # Combine the parts with a slash
    return f"{formatted_date}{ticks_since_epoch}/"

# Example usage:
current_time = datetime.now()
result = convert_pipeline_expression(current_time)
print(result)
```

This Python code:
1. Takes a datetime object as input (equivalent to pipeline().TriggerTime)
2. Formats the date part using strftime() (equivalent to formatDateTime())
3. Calculates the ticks since Unix epoch and divides by 10000000 (equivalent to the div/sub/ticks operations)
4. Concatenates the parts with a slash

The main differences to note:
- Python uses datetime objects instead of ADF's pipeline().TriggerTime
- The ticks calculation is simplified to use Python's datetime arithmetic
- String concatenation is done using f-strings instead of concat()

Would you like me to explain any part of this conversion in more detail?