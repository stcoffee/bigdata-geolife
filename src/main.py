import argparse

from src.jobs.trip_statistics import TripStatisticsJob
from src.jobs.daily_statistics_job import DailyStatisticsJob


def main():
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--sample-data', type=str, required=True, dest='sample-data', help="Input folder")
    parser.add_argument('--output', type=str, required=True, dest='output', help="Output folder")
    parser.add_argument('--job', type=str, required=True, dest='job_name', help="Job name")
    parser.add_argument('--debug', required=False, action='store_true', default=False, help="Enable debug mode")
    args = parser.parse_args()

    match args.job_name:
        case "daily_statistics":
            dc = DailyStatisticsJob(args.input, args.output, write_intermediate=args.debug)
            dc.run_job()
        case "trip_statistics":
            ts = TripStatisticsJob(args.input, args.output, write_intermediate=args.debug)
            ts.run_job()


if __name__ == '__main__':
    main()
