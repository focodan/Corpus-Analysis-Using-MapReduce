# Given:  <year> <score>
# Output: <year> <score'> where <score'> is aggregated per decade 

def toFields(line):
  return [ x.strip(' ') for x in line.split(',') if x != '' and x!= ' ' and x!=',' ]

def toCSV(s1,s2):
  return s1+','+s2+'\n'


def aggregate(inFileName):
  print inFileName
  outFileName = 'aggregated'+inFileName
  inFile = open(inFileName,'r')
  
  #variables for aggregation
  curYear = 0
  curDec = 0
  curDecSum = 0
  curDecCnt = 1 # avoid divide by 0
  agList = []
  for line in inFile:
    #print line
    fields = toFields(line)
    curYear = int(fields[0])
    if curYear/10 == curDec: #same decade, keep running total
      curDecSum += float(fields[1])
      curDecCnt += 1
    else: #write out old total, start new running total
      agList.append([curDec*10,curDecSum/float(curDecCnt)])
      curDec = curYear/10
      curDecSum = float(fields[1])
      curDecCnt = 1
    pass
  agList.append([curDec*10,curDecSum/float(curDecCnt)]) # don't forget last line
  
  #write the aggregates
  outFile = open(outFileName,'w')
  for dat in agList:
    outFile.write(toCSV(str(dat[0]),str(dat[1])))
    pass
  pass


def main():
  aggregate("readingScoresFRE.csv")
  aggregate("readingScoresGRL.csv")
  print "done"
  pass

main()
