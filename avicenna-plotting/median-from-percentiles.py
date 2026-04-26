import sys

if __name__ == '__main__':
    f = open(sys.argv[1], 'r')
    percentiles = []
    for line in f.readlines():
        percentiles = [float(x) for x in line.split()]
    # print(percentiles)
    print(str(percentiles[0]) + ' ' + str(percentiles[50]))