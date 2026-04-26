import sys

if __name__ == '__main__':
    f = open(sys.argv[1], 'r')
    # out = open(sys.argv[2], 'w')
    nTrials = int(sys.argv[2])

    allTputLats = []
    for line in f.readlines():
        allTputLats.append([float(x) for x in line.split()])
    latTrials = []
    tputTrials = []
    i = 0
    while i <= len(allTputLats)-nTrials:
        for j in range(0, nTrials):
            tputTrials.append(allTputLats[i+j][0])
            latTrials.append(allTputLats[i+j][50])
            # print(str(tputTrials[j]) + " " + str(latTrials[j]))
        tputTrials.sort()
        latTrials.sort()
        if nTrials%2 == 1:
            medianTput = tputTrials[int((nTrials-1)/2)]
            medianLat = latTrials[int((nTrials-1)/2)]
            print(str(medianTput) + ' ' + str(medianLat))
        else:
            medianTput = (tputTrials[int((nTrials-1)/2)] +  tputTrials[int((nTrials-1)/2 + 1)])/2.0
            medianLat = (latTrials[int((nTrials-1)/2)] +  latTrials[int((nTrials-1)/2 + 1)])/2.0
            print(str(medianTput) + ' ' + str(medianLat))
        tputTrials = []
        latTrials = []
        i += nTrials


    # percentiles = []
    # for line in f.readlines():
    #     percentiles.append(float(line))
    # print(percentiles[49]) # 50th percentile is the median, 99 is 100th percentile
    # tps = []
    # lats = []

    # out.write('"1 Multi-Key Operation"\n')
    # pgs = 1
    # trial = 0



    # for line in f.readlines():
    #     if line == '\n':
    #         assert trial == 0

    #         pgs += 1
    #         if pgs == 9:
    #             break
    #         # if pgs == 32:
    #         #     # actually shouldn't even execute
    #         #     pgs = 24
    #         #     #break
    #         # elif pgs == 48:
    #         #     break

    #         out.write('\n\n"%d Multi-Key Operations"\n' % pgs)
    #         continue


    #     tplat = line.split()
    #     tps.append(float(tplat[0]))
    #     lats.append(float(tplat[1]))

    #     if trial == 4:
    #         print('on trial 5')
    #         # sort
    #         tps.sort()
    #         lats.sort()
    #         # print out median
    #         # appends per second is size of mk_op (pgs) + commit
    #         # reads are just nops
    #         out.write("%f %f %f\n" % (tps[2], lats[2], float(tps[2])*(int(sys.argv[3]))))

    #         #reset vars
    #         tps = []
    #         lats = []
    #         trial = 0
    #     else:
    #         trial += 1


    # print('done')