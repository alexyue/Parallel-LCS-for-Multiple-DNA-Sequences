#!/usr/bin/env python
import mincemeat
import math
from psim import PSim

data = ["ACTTCAGGCTAA",
        "TTCTAAAGCAAT",
        "AACCGTATTCGA"
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
    maxLength = max([len(arr[0]), len(arr[1]), len(arr[2])])
    
    #fill in missing lengths
    for i in range(len(arr)):
        l = len(arr[i])
        if len(arr[i]) < maxLength:
            s = maxLength - l
            for j in range(s, maxLength):
                arr[i].append(0)

    X = arr[0][r] + arr[1][r] + arr[2][r]
    Y = math.fabs(arr[0][r] - arr[1][r]) + math.fabs(arr[1][r] - arr[2][r])  
    Z = X + Y

    return Z  


def GetIJK(r, arr):
    newRow = []
    for i in range(len(arr)):
        newRow.append(arr[i][r])

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

Za = CalcXYZ(0,Ba)
Zc = CalcXYZ(0,Bc)
Zt = CalcXYZ(0,Bt)
Zg = CalcXYZ(0,Bg)

newZ = {Za:"A", Zc:"C", Zt:"T", Zg:"G"}
minZ = min(Za, Zc, Zt, Zg)

letter = newZ[minZ]

if letter == "A":
    arr = Ba
elif letter == "C":
    arr = Bc
elif letter == "T":
    arr = Bt
elif letter == "G":
    arr = Bg

newRow = GetIJK(0, arr)

print newRow
    





