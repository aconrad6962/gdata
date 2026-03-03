#
#  Retrieve data collected for UM-2025B-002
#  Version 3   Nov 28, 2025
#
import pyvo
import requests
import shutil
from datetime import datetime
from pathlib import Path
from sys import argv

script, token_path = argv
program_id = "AZ-2024B-018"
obs_date = "250116"
start_file = 3000
end_file = 5000


def glog(log_file, string):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_file.write(timestamp + ": " + string)
    log_file.flush()


print("gdata does not write to the terminal unless there is an error.")
print('To monitor status, type "tail -f gdata.log" in another terminal session.')

#
# Construct the service object
#
access_url = "https://archive.lbto.org/tap"
service = pyvo.dal.TAPService(access_url)

#
# Set up logging
#
log_file = open("gdata.log", "a", buffering=1)
glog(log_file, "------------- The gdata program was restarted ------------\n")
glog(log_file, "gdata version 2.0.  November 26, 2025\n")

#
# Form the query
#

query = f"""SELECT lbt.file_name AS file_name FROM lbt.lbt lbt WHERE lbt.program = '{program_id}' AND lbt.file_name LIKE '%lm\_%'"""

#
# Token is token downloaded from the LBTO archive
#
token = open(token_path).read().strip()
headers = {"Authorization": f"Bearer {token}"}

#
# Issue the query
#
result = service.run_async(query)

current_dir = Path.cwd()  # current directory
#
# For each file, check if already retrieved in a previous run.
# If so, continue, else, retrieve it.
#
glog(log_file, "Total files found = " + str(len(result)) + "\n")
for row in result:
    filename = row["file_name"]
    file_path = current_dir / filename

    date = filename.split("_")[1]
    if date != obs_date:
        glog(log_file, "skipping " + filename + " because it's from the wrong day\n")
        continue

    file_number = int(filename.split("_")[-1].split(".")[0])

    if file_number < start_file or file_number > end_file + 1:
        glog(
            log_file, "skipping " + filename + " because file number is out of range\n"
        )
        continue

    if file_path.exists():
        glog(log_file, "skipping " + filename + " because it already exists\n")
        continue
    else:
        glog(log_file, filename + " retrieval started\n")

        # Form the URL
        url = (
            "https://archive.lbto.org/files/lbt"
            + filename
        )

        # Retrieve the file
        r = requests.get(url, headers=headers, stream=True)
        try:
            r.raise_for_status()
        except Exception as e:
            glog(log_file, "failed {url}: {e!r}")
            continue
        with open(filename, "wb+") as fd:
            shutil.copyfileobj(r.raw, fd)

        glog(log_file, filename + " retrieval complete\n")

