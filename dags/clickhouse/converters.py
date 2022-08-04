import datetime

EPOCH_START = datetime.date(1970, 1, 1)
def get_count_days(dt) -> int:
    if not dt:
        return 0

    date = dt.date() if isinstance(dt, datetime.datetime) else dt

    return (date - EPOCH_START).days


def apply_timestamp_by_datetime(row):
    if not row:
        return ''
    return datetime.datetime.strptime(row.split('.')[0], '%Y-%m-%dT%H:%M:%S').timestamp()
