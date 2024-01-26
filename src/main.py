import argparse
import time

from src.jobs.transportation_mode_statistics import TransportationStatisticsJob
from src.jobs.daily_statistics import DailyStatisticsJob
from src.jobs.transportation_modes import TransportationModesJob


def main():
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--input', type=str, required=True, dest='input', help="Input folder")
    parser.add_argument('--output', type=str, required=True, dest='output', help="Output folder")
    parser.add_argument('--job', type=str, required=True, dest='job_name', help="Job name: daily_statistics (ds) or "
                                                                                "transportation_statistics (ts) or "
                                                                                "transportation_modes (tm)")
    parser.add_argument('--debug', required=False, action='store_true', default=False, help="Enable debug mode")
    args = parser.parse_args()

    print(args)

    start_time = time.time()

    if args.job_name == "daily_statistics" or args.job_name == "ds":
        dc = DailyStatisticsJob(args.input, args.output, debug=args.debug)
        dc.run_job()
    elif args.job_name == "transportation_statistics" or args.job_name == "ts":
        ts = TransportationStatisticsJob(args.input, args.output, debug=args.debug)
        ts.run_job()
    elif args.job_name == "transportation_modes" or args.job_name == "tm":
        tm = TransportationModesJob(args.input, args.output, debug=args.debug)
        tm.run_job()

    print(f"Execution time {time.time() - start_time}s")


if __name__ == '__main__':
    main()
