#Code here
x = sc.textFile('studentsPR.csv')
intermidiate = x.map(lambda line: line.split(","))
res = intermidiate.filter(lambda x: x[2]=='71381' and x[5]=='F')
res.collect()
