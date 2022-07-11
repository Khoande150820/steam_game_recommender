import sys


def show_work_status(singleCount, totalCount, currentCount):
    currentCount += singleCount
    percentage = 1. * currentCount / totalCount * 100
    status = '>' * int(percentage / 2) + '-' * (50 - int(percentage / 2))
    if percentage % 3:
        sys.stdout.write(f'\r Status:[{status}]{percentage:.2f}')
        sys.stdout.flush()
    if percentage >= 100:
        print('\n')
