import argparse
import time

from src.jobs.trip_statistics import TripStatisticsJob
from src.jobs.daily_statistics_job import DailyStatisticsJob
from src.jobs.transportation_modes import TransportationModesJob


def main():
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--input', type=str, required=True, dest='input', help="Input folder")
    parser.add_argument('--output', type=str, required=True, dest='output', help="Output folder")
    parser.add_argument('--job', type=str, required=True, dest='job_name', help="Job name")
    parser.add_argument('--debug', required=False, action='store_true', default=False, help="Enable debug mode")
    args = parser.parse_args()

    print(args)

    start_time = time.time()

    match args.job_name:
        case "daily_statistics":
            dc = DailyStatisticsJob(args.input, args.output, debug=args.debug)
            dc.run_job()
        case "trip_statistics":
            ts = TripStatisticsJob(args.input, args.output, debug=args.debug)
            ts.run_job()
        case "transportation_modes":
            tm = TransportationModesJob(args.input, args.output, debug=args.debug)
            tm.run_job()

    print(f"Execution time {time.time() - start_time}s")


if __name__ == '__main__':
    main()
