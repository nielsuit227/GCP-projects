import timeit, joblib
import numpy as np
from sklearn import svm, datasets


input, output = datasets.make_classification(
    n_features=50,
    n_informative=20,
    n_clusters_per_class=20,
    n_classes=2,
    n_samples=400
)
model = svm.SVC(kernel='rbf').fit(input, output)
joblib.dump(model, '../svmtimer.joblib')



func = 'model.predict(input)'
setup = '''
import joblib, numpy
model = joblib.load('svmtimer.joblib')
input = numpy.random.normal(0, 1, 50).reshape((1, 50))
print('Setup Complete')
'''
t = timeit.repeat(func, setup=setup, number=10000, repeat=5)
print(t)