#!/usr/bin/env python
import mincemeat
import math
from psim import PSim
import sys

#TODO:  Fix hardcode for 3 string
#       Fix if no A,C,G, or T in string
#       Generalize more?

data = ["AATCG",
        "TACGG",
        "CAGCG"
        ]


#For each string, map each letter to the index
def mapfn(k, v):
    l = len(v)

    for w in range(l):
        yield k, {v[w] : w}

#reduce to 4 arrays
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

def CalcXYZ(r, arr):
    X = 0
    Y = 0
    Z = 0

    l = len(r)
    for i in range(l):
        X += arr[i][r[i]]

    for i in range(l - 1):
        Y += math.fabs(arr[i][r[i]] - arr[i + 1][r[i+1]])

   # X = arr[0][r] + arr[1][r] + arr[2][r]
   # Y = math.fabs(arr[0][r] - arr[1][r]) + math.fabs(arr[1][r] - arr[2][r])  
    Z = X + Y

    return Z  



def UnjagArray(arr):
    maxLength = max([len(arr[0]), len(arr[1]), len(arr[2])])

    #fill in missing lengths
    for i in range(len(arr)):
        l = len(arr[i])
        if l < maxLength:
            s = maxLength - l
            for j in range(s):
                arr[i].append(0)


def GetIJK(r, arr):
    newRow = []
    l = len(r)
    for i in range(l):
        newRow.append(arr[i][r[i]])

    return newRow

def GetNextIndexes(r, arr):
    newIndexes = []
    l = len(r) #num strings

    for i in range(l):
        for j in range(len(arr[i])):
            if arr[i][j] > r[i]:
                #newIndexes.append(arr[i][j])
                newIndexes.append(j)
                break

    return newIndexes

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


s = mincemeat.Server()

# The data source can be any dictionary-like object
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

comm = PSim(5)  #4 bases

newPosa = [0,0,0]
newPosc = [0,0,0]
newPost = [0,0,0]
newPosg = [0,0,0]


while True:

    if comm.rank == 0:
        newRow = GetLetter(newPosa, newPosc, newPost, newPosg)

        newPosa = GetNextIndexes(newRow, Ba)
        newPosc = GetNextIndexes(newRow, Bc)
        newPost = GetNextIndexes(newRow, Bt)
        newPosg = GetNextIndexes(newRow, Bg)
       
        if len(newPosa) < 3 and len(newPosc) < 3 and len(newPost) < 3 and len(newPosg) < 3:  #TODO: Fix 3
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

        if len(newPos) < 3:  #TODO: Fix 3
            Z = sys.maxint
        else:
            Z = CalcXYZ(newPos, B)

        comm.send(0, Z)




