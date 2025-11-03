import argparse
import os
import awswrangler as wr


def get_overnight_data(date, client):
    """
    Downloads files from an S3 folder to a local folder based on the provided date and client name.

    Args:
        date (str): The date in YYYYMMDD format.
        client_name (str): The name of the client.
    """
    date = date.replace("-", "")  # Ensure date is in YYYYMMDD format
    s3_folder = 's3://d1g1t-custodian-data-us-east-1/apx/gresham/YYYYMMDD/'
    s3_folder = s3_folder.replace("YYYYMMDD", date)

    # Step 1: Define the local folder where files will be downloaded
    local_folder = os.path.join("get_overnight_data/outputs", client, date)
    # os.makedirs(local_folder, exist_ok=True)  # Create the local folder if it doesn't exist

    # print(f"Downloading files from S3 folder: {s3_folder}")
    # print(f"Saving files to local folder: {local_folder}")

    # Step 4: List all files in the S3 folder
    try:
        s3_files = wr.s3.list_objects(s3_folder)
        if not s3_files:
            print(f"No files found in S3 folder: {s3_folder}")
            return
        else:
            print(f"Downloading files from S3 folder: {s3_folder}")
            print(f"Saving files to local folder: {local_folder}")
            os.makedirs(
                local_folder, exist_ok=True
            )  # Create the local folder if it doesn't exist

        # Step 5: Download each file to the local folder
        for s3_file in s3_files:
            filename = s3_file.replace(".csv", f"_{date}.csv")
            local_file_path = os.path.join(local_folder, os.path.basename(filename))
            print(f"Downloading {s3_file} to {local_file_path}")
            wr.s3.download(path=s3_file, local_file=local_file_path)

        print("All files downloaded successfully!")

    except Exception as e:
        print(f"An error occurred while accessing S3: {e}")


if __name__ == "__main__":
    # Step 1: Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Download files from S3 based on date and client name."
    )
    parser.add_argument(
        "-d", "--date", type=str, required=True, help="Date in YYYYMMDD format."
    )
    parser.add_argument(
        "-c", "--client", type=str, required=True, help="Name of the client."
    )
    args = parser.parse_args()

    # Step 2: Call the function to download files
    get_overnight_data(args.date, args.client)
