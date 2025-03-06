import subprocess
import time
import os

def launch_batch_file(file_path):
    # Get the directory where the batch file is located
    dir_path = os.path.dirname(file_path)
    # Launch the process with the specified working directory
    process = subprocess.Popen(file_path, cwd=dir_path, shell=True)
    return process

def main():
    # Paths for the batch files
    mongodb_start = r".\mongodb.bat"
    jupiter_start_all = r".\start_instances.bat"
    jupiter_health_check = r".\health_check.bat"

    # Start the first process (MongoDB)
    print("Starting MongoDB...")
    process1 = launch_batch_file(mongodb_start)

    # Wait for 5 seconds
    time.sleep(5)

    # Start the second process (Jupiter start_all)
    print("Starting Jupiter start_all...")
    process2 = launch_batch_file(jupiter_start_all)

    # Wait for 5 seconds
    time.sleep(5)

    # Start the third process (Jupiter health_check)
    print("Starting Jupiter health_check...")
    process3 = launch_batch_file(jupiter_health_check)

    print("All processes have been started.")

if __name__ == "__main__":
    main()
