from functools import wraps
import logging

def timer(original_function):
    """
    Calculate how much time `original_function` took to finish.
    """

    import datetime

    def wrapper(*args, **kwargs):        
        start = datetime.datetime.now()
        value = original_function(*args, **kwargs)
        total_time = datetime.datetime.now() - start

        logging.info("Finished executing %s function. Process took %s seconds", original_function.__name__, f"{total_time.seconds}.{total_time.microseconds // 10}")
        return value
    
    return wrapper


def logger(original_function):
    """
    Log the important details about the function including the arguments used, number of times it is called, and the amount of time used.
    """

    # Configure Logging    
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
    amount_executed = 0

    # Convert integer into ordinal number. E.g. 1 -> "1st", 2 -> "2nd", ...
    def _ordinal(n):
        if 11 <= (n % 100) <= 13:
            suffix = 'th'
        else:
            suffix = ['th', 'st', 'nd', 'rd', 'th'][min(n % 10, 4)]
        
        return str(n) + suffix
    

    # Use wraps to preserve orginal function name then timer to calculate how long the process took
    @timer
    @wraps(original_function)
    def wrapper(*args, **kwargs):        
        # Increase the number of amounts the function get called
        nonlocal amount_executed
        amount_executed += 1        

        # Extra information about the function
        args_str = ', '.join(repr(i) for i in args)
        kwargs_str = ', '.join(f"{k} = {repr(v)}" for k,v in kwargs.items())

        # Log the information before running
        logging.info("Running %s(%s). `%s` function called for the %s time", original_function.__name__, args_str + kwargs_str, original_function.__name__,  _ordinal(amount_executed))        
        return original_function(*args, **kwargs)       

    return wrapper