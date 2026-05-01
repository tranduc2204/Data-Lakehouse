import os

def validate_file(file_path):
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' does not exist.")
        return False
    if not file_path.endswith('.csv'):
        print(f"Error: File '{file_path}' is not a CSV file.")
        return False
    if os.path.getsize(file_path) == 0:
        print(f"Error: File '{file_path}' is empty.")
        return False
    return True