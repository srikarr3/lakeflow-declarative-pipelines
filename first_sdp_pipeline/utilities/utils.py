from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import re

# ==============================================================================
# PIPELINE UTILITIES & CUSTOM USER-DEFINED FUNCTIONS (UDFs)
# ==============================================================================
# Declarative pipelines allow the import and usage of standard Python modules 
# and custom PySpark UDFs for specialized validation and transformations.
# ==============================================================================

@udf(returnType=BooleanType())
def is_valid_email(email):
    """
    Checks if a string email address matches a standardized RFC 5322 regex pattern.
    
    Args:
        email (str): The email string to evaluate.
        
    Returns:
        bool: True if the format is valid, False otherwise.
    """
    # Standard RFC-compliant email regex pattern
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    if email is None:
        return False
        
    # Evaluate regex match and return Boolean flag
    return re.match(pattern, email) is not None
