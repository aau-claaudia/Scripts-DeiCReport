import argparse
import csv
import json
import datetime
import hashlib
import copy

parser = argparse.ArgumentParser(description='Do stuff')
parser.add_argument('input', type=str,
                    help='filename with input')

args = parser.parse_args()

UNIVERSITY_ID = 5
TYPE_1_HPC_CENTER_ID = "f0679faa-242e-11eb-3aba-b187bcbee6d4"
TYPE_1_HPC_SUB_CENTER_ID_AAU = "1003d37e-242f-11eb-186e-0722713fb0ad"
AAU_TYPE_1_CLOUD_PROJECT_ID = "550f9fde-2411-4731-973a-2afb2c61e971"


def read_file(filename):
    with open(filename) as f:
        return f.read()


def parse_input(filename):
    input = []
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data = json.loads(row['json'])
            data["date"] = row['Date']

            input.append(data)

    return input


def get_orcid(username):
    hash_object = hashlib.sha256(username.encode())
    return hash_object.hexdigest()


def create_center_summary(input):
    instances = get_instances(input)
    center = handle_instances(instances)

    return center


def get_instances(input):
    instances = []

    for i in input:
        date = datetime.datetime.strptime(i["date"], "%d-%m-%Y")

        if i["request"] == "creation":
            end_date = datetime.datetime.now() + datetime.timedelta(days=1)
            end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

            others = filter(lambda x: x["job_id"] == i["job_id"] and x["request"] == "deletion", input)
            deletion = next(others, None)

            if deletion:
                end_date = datetime.datetime.strptime(deletion["date"], "%d-%m-%Y")

            i["endDate"] = end_date
            i["date"] = date

            instances.append(i)

    return instances


def create_persons(input):
    persons_summaries = []
    daily_persons_summaries = []

    persons = set([i["owner_username"] for i in input])

    for person in persons:
        person_requests = filter(lambda x: x["owner_username"] == person, input)

        instances = get_instances(person_requests)

        if instances:
            # This is total for a user
            result = handle_instances(instances)

            person_summary = {
                "deicProjectId": AAU_TYPE_1_CLOUD_PROJECT_ID,
                "hpcCenterId": TYPE_1_HPC_CENTER_ID,
                "subHPCCenterId": TYPE_1_HPC_SUB_CENTER_ID_AAU,
                "universityId": UNIVERSITY_ID,
                "orcid": get_orcid(person),
                "accessType": 1,
                "accessStartDate": result["startPeriod"],
                "accessEndDate": result["endPeriod"],
                "cpuCoreTimeAssigned": result["maxCPUCoreTime"],
                "cpuCoreTimeUsed": result["usedCPUCoreTime"],
                "gpuCoreTimeAssigned": result["maxGPUCoreTime"],
                "gpuCoreTimeUsed": result["usedGPUCoreTime"],
                "storageAssignedInMB": result["storageUsedInMB"],
                "storageUsedInMB": result["storageUsedInMB"],
                "nodeTimeAssigned": None,
                "nodeTimeUsed": None
            }

            persons_summaries.append(person_summary)

            # This is for each day
            person_dailys = create_daily_summaries(instances)
            for p in person_dailys:
                center_daily = {
                    "hpcCenterId": TYPE_1_HPC_CENTER_ID,
                    "subHPCCenterId": TYPE_1_HPC_SUB_CENTER_ID_AAU,
                    "date": p["startPeriod"],
                    "orcid": get_orcid(person),
                    "deicProjectId": AAU_TYPE_1_CLOUD_PROJECT_ID,
                    "universityId": UNIVERSITY_ID,
                    "maxCPUCoreTime": p["maxCPUCoreTime"],
                    "usedCPUCoretime": p["usedCPUCoreTime"],
                    "maxGPUCoreTime": p["maxGPUCoreTime"],
                    "usedGPUCoretime": p["usedGPUCoreTime"],
                    "storageUsedInMB": p["storageUsedInMB"],
                    "networkUsageInMB": None,
                    "networkAvgUsage": None,
                    "maxNodeTime": None,
                    "usedNodeTime": None,
                    "accessType": 1
                }

                daily_persons_summaries.append(center_daily)

    return persons_summaries, daily_persons_summaries


def create_daily_summaries(instances):
    daily_centers = []

    start_date = min([i["date"] for i in instances])

    for d in range((datetime.datetime.now() - start_date).days + 1):
        day = start_date + datetime.timedelta(days=d)
        end_day = day + datetime.timedelta(days=1)

        day_instances = list(filter(lambda x: x["date"] <= day and x["endDate"] >= day, instances))

        for d in day_instances:
            d["endDate"] = end_day
            d["date"] = day

        if day_instances:
            center_daily = handle_instances(day_instances)

            center_daily

            daily_centers.append(center_daily)

    return daily_centers


def handle_instances(input):
    center = {
        "hpcCenterId": "",
        "subHPCCenterId": "",
        "startPeriod": None,
        "endPeriod": None,
        "maxCPUCoreTime": 0,
        "usedCPUCoreTime": 0,
        "maxGPUCoreTime": 0,
        "usedGPUCoreTime": 0,
        "storageUsedInMB": 0,
        "netWorkUsageInMB": 0,
        "netWorkAvgUsage": 0,
        "maxNodeTime": None,
        "usedNodeTime": None
    }

    for i in input:
        if not center["startPeriod"] or i["date"] < center["startPeriod"]:
            center["startPeriod"] = i["date"]

        if not center["endPeriod"] or i["endDate"] > center["endPeriod"]:
            center["endPeriod"] = i["endDate"]

        hours = (i["endDate"] - i["date"]).total_seconds() / 3600

        core_hours = hours * flavors()[i["machine_template"]]["cpu"]

        center["maxCPUCoreTime"] += core_hours
        center["usedCPUCoreTime"] += core_hours

        if flavors()[i["machine_template"]]["gpu"]:
            center["maxGPUCoreTime"] += hours
            center["usedGPUCoreTime"] += hours

        center["storageUsedInMB"] += flavors()[i["machine_template"]]["storage_tb"] * 1000000

    if center["startPeriod"]:
        center["startPeriod"] = center["startPeriod"].strftime("%Y-%m-%d")
    if center["endPeriod"]:
        center["endPeriod"] = center["endPeriod"].strftime("%Y-%m-%d")

    return center


def flavors():
    return {
        "uc-general-small": {
            "cpu": 4,
            "ram_gb": 16,
            "storage_tb": 1,
            "gpu": False},
        "uc-general-medium": {
            "cpu": 8,
            "ram_gb": 32,
            "storage_tb": 1,
            "gpu": False},
        "uc-general-large": {
            "cpu": 16,
            "ram_gb": 64,
            "storage_tb": 1,
            "gpu": False},
        "uc-general-xlarge": {
            "cpu": 64,
            "ram_gb": 256,
            "storage_tb": 1,
            "gpu": False},
        "uc-t4-1": {
            "cpu": 10,
            "ram_gb": 40,
            "storage_tb": 1,
            "gpu": True
        }
    }


def main():
    print(f"Collecting data from: {args.input}")

    input = parse_input(args.input)

    center_summary = create_center_summary(copy.deepcopy(input))
    with open('Center.json', 'w') as outfile:
        json.dump(center_summary, outfile, indent=4)
        print(f"Wrote: {outfile.name}")

    person, daily = create_persons(copy.deepcopy(input))
    with open('Person.json', 'w') as outfile:
        json.dump(person, outfile, indent=4)
        print(f"Wrote: {outfile.name}")

    with open('CenterDaily.json', 'w') as outfile:
        json.dump(daily, outfile, indent=4)
        print(f"Wrote: {outfile.name}")


if __name__ == "__main__":
    main()
