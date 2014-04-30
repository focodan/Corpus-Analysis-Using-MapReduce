# Given: <filename> <score1> <score2>
# FRE:   <year> <score1>
# FGL:   <year> <score2>

inFile = open("readingScoresSorted.csv","r") #perhaps add buffering
outFRE = open("readingScoresFRE.csv","w")
outFGL = open("readingScoresGRL.csv","w")

def toFields(line):
  return [ x.strip(' ') for x in line.split(',') if x != '' and x!= ' ' and x!=',' ]

# Takes book title, and returns the year the book was published as a str
def titleToYear(title):
  yearRaw = (title[8:]).split('.')[0] # gives 'yyyy' or 'yyyyBC'
  if yearRaw.find('BC') == -1: # there's no BC in the year
    return yearRaw
  else: #we need to remove 'BC', convert to int, and make negative
    return str(-1*int(yearRaw[:-2]))

def toCSV(s1,s2):
  return s1+','+s2+'\n'

for line in inFile:
  fields = toFields(line)
  year = titleToYear(fields[0])
  scoreFRE = fields[1]
  scoreFGL = fields[2]
  outFRE.write(toCSV(year,scoreFRE))
  outFGL.write(toCSV(year,scoreFGL))
  pass

print "done"
