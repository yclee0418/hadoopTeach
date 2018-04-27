set PATH=%PATH%;C:\dev\spark-2.2.1\bin
set PATH=%PATH%;C:\Users\Y04547\AppData\Local\Continuum\anaconda3
set ANACONDA_PATH=C:\Users\Y04547\AppData\Local\Continuum\anaconda3
set PYSPARK_DRIVER_PYTHON=%ANACONDA_PATH%\Scripts\ipython
set PYSPARK_PYTHON=%ANACONDA_PATH%\python
setx PYSPARK_DRIVER_PYTHON ipython
setx PYSPARK_DRIVER_PYTHON_OPTS notebook
pyspark
