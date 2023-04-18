import os
def run():

    try:
        print('import pyspark.boto3')
        import pyspark.boto3
    except:

        os.system('pip install boto3')
        print('Installed the boto3')
    import boto3

    boto3_path = os.path.dirname(boto3.__file__)

    import pyspark
    source_path = os.path.dirname(pyspark.__file__)


    boto3_folder = "boto3"

    try:
        boto3_source_path=os.path.join(source_path,boto3_folder)

    except:
        pass

    import shutil

    try:

        shutil.copytree(boto3_path,boto3_source_path)

    except:

        pass





    #import os
    try:
        import pyspark.tensorflow
    except:
        os.system('pip install tensorflow')
    import tensorflow
    tensorflow_path = os.path.dirname(tensorflow.__file__)
    import pyspark
    source_path = os.path.dirname(pyspark.__file__)

    ts_folder = "tensorflow"

    #try:
    tensorflow_source_path=os.path.join(source_path,ts_folder)
    #except:
    #    pass

    import shutil

    try:
        print('Tensorflow is installing')
        shutil.copytree(tensorflow_path,tensorflow_source_path)
    except:

        pass
