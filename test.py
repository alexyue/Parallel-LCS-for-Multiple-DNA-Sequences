#Alex Yue
#CSC 503
#Final Project - LCS in Multiple DNA Sequences
#
#
#This program is used to determine the longest common subsequence (LCS) found between multiple DNA
#sequences.  The algorithm is based on the following research paper here:
#    www.niitcrcs.com/iccs/papers/2005_48.pdf
#
#Some improvements were made in addition to the algorithm, in particular the initialization of the
#"buckets".  Using Map/Reduce we can parallelize the filling of the buckets to keep the runtime down.
#
#This program assumes that we are only working with nucleotides (4 bases, A,C,T,G), although could 
#be generalized to a more generic alphabet.

#!/usr/bin/env python
import mincemeat
import math
import sys
from psim import PSim

#TODO:  
#       Fix if no A,C,G, or T in string
#       Generalize more?

data = ["ACTTCAGGCTAA",
        "TTCTAAAGCAAT",
        "AACCGTATTCGA",
        "AAGCTGAACGAT"
        ]


#Map Function - 
#  This is the mapping function used to map the index positions of each unique letter in the
#  set of data.  Each index for each letter is mapped for each string that is to be compared.
def mapfn(k, v):
    l = len(v)

    for w in range(l):
        yield k, {v[w] : w}

#Reduce Function - 
#  This is the reduction function that is used to reduce the mapped indexes for each unique letter
#  in the strings and break them out into "buckets/arrays" of each nucleotide.  This will reduce the 
#  mapped data into 4 lists for each nucleotide.
def reducefn(k, vs):
    Ba = []
    Bc = []
    Bt = []
    Bg = []

    l = len(vs)
    for i in range(l):
        for y,z in vs[i].iteritems():
            if vs[i].keys()[0] == "A":
                Ba.append(z)
            elif vs[i].keys()[0] == "C":
                Bc.append(z)
            elif vs[i].keys()[0] == "T":
                Bt.append(z)
            elif vs[i].keys()[0] == "G":
                Bg.append(z)
 
    return {"A": Ba}, {"C": Bc} , {"T": Bt}, {"G": Bg}

#Method calculates Z value which is used in the algorithm to figure out
#which nucleotide is part of the subsequence.
def CalcXYZ(r, arr):
    X = 0
    Y = 0
    Z = 0

    l = len(r)
    for i in range(l):
        X += arr[i][r[i]]

    for i in range(l - 1):
        Y += math.fabs(arr[i][r[i]] - arr[i + 1][r[i+1]])

    Z = X + Y

    return Z  


#Method "Unjags" the 2D array to ensure that each column in the array has the same length
#This helps in the algorithm to avoid any indexing issues.  We append a 0 to the end of the arrays
def UnjagArray(arr):
    arrLengths = []

    for i in range(len(arr)):
        arrLengths.append(len(arr[i]))    

    maxLength = max(arrLengths)

    #fill in missing lengths
    for i in range(len(arr)):
        l = len(arr[i])
        if l < maxLength:
            s = maxLength - l
            for j in range(s):
                arr[i].append(0)

#Method gets the indexes for the strings that were used in calculating the selected Z
def GetIJK(r, arr):
    newRow = []
    l = len(r)
    for i in range(l):
        newRow.append(arr[i][r[i]])

    return newRow

#Get the next indexes that are greater than what is passed in r
def GetNextIndexes(r, arr):
    newIndexes = []
    l = len(r) #num strings

    for i in range(l):
        for j in range(len(arr[i])):
            if arr[i][j] > r[i]:
                newIndexes.append(j)
                break

    return newIndexes

#Method does the work to get the next letter in the LCS
def GetLetter(newPosa, newPosc, newPost, newPosg):

    #Send positions to each process
    comm.send(1, newPosa)
    comm.send(2, newPosc)
    comm.send(3, newPost)
    comm.send(4, newPosg)

    #receive Z's
    Za = comm.recv(1)        
    Zc = comm.recv(2)
    Zt = comm.recv(3)
    Zg = comm.recv(4)

    #Get min Z and print letter
    newZ = {Za:"A", Zc:"C", Zt:"T", Zg:"G"}
    minZ = min(Za, Zc, Zt, Zg)
    
    letter = newZ[minZ]

    if letter == "A":
        arr = Ba
        nextPos = newPosa
    elif letter == "C":
        arr = Bc
        nextPos = newPosc
    elif letter == "T":
        arr = Bt
        nextPos = newPost
    elif letter == "G":
        arr = Bg
        nextPos = newPosg

    print letter

    #get next index
    newRow = GetIJK(nextPos, arr) 

    return newRow


#Begin work using mincemeat to map/reduce the passed in data
s = mincemeat.Server()

s.datasource = dict(enumerate(data))
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

#Compile results into 4 arrays
Ba = []
Bc = []
Bg = []
Bt = []

for k,v in results.iteritems():
    #fill A
    for s,t in v[0].iteritems():               
        Ba.append(t)

    #fill C
    for s,t in v[1].iteritems():
        Bc.append(t)

    #fill T
    for s,t in v[2].iteritems():
        Bt.append(t)

    #fill G
    for s,t in v[3].iteritems():
        Bg.append(t)

UnjagArray(Ba)
UnjagArray(Bc)
UnjagArray(Bt)
UnjagArray(Bg)

#Number of strings
numStrings = len(Ba)

newPosa = []
newPosc = []
newPost = []
newPosg = []

#build initial index seed
for i in range(numStrings):
    newPosa.append(0)  
    newPosc.append(0)  
    newPost.append(0)  
    newPosg.append(0)  

#Now we parallelize the calculating of the Z values
#We create 5 seperate processes, 1 master process to direct and control
#and 4 other processes to each calculated the Z values
comm = PSim(5)  #4 bases

while True:

    if comm.rank == 0:
        newRow = GetLetter(newPosa, newPosc, newPost, newPosg)

        newPosa = GetNextIndexes(newRow, Ba)
        newPosc = GetNextIndexes(newRow, Bc)
        newPost = GetNextIndexes(newRow, Bt)
        newPosg = GetNextIndexes(newRow, Bg)
       
        if len(newPosa) < numStrings and \
           len(newPosc) < numStrings and \
           len(newPost) < numStrings and \
           len(newPosg) < numStrings:  
            break
    else:
        #receive arrays
        newPos = comm.recv(0)

        if comm.rank == 1:
            B = Ba
        elif comm.rank == 2:
            B = Bc
        elif comm.rank == 3:
            B = Bt
        elif comm.rank == 4:
            B = Bg

        if len(newPos) < numStrings:  
            Z = sys.maxint
        else:
            Z = CalcXYZ(newPos, B)

        comm.send(0, Z)




