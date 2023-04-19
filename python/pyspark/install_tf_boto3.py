import os
def run():
#installation for boto3
    try:
        import pyspark.boto3
    except:
        print('Installing boto3...')
        os.system('pip install boto3 -q -q -q')
        print('Installed boto3')
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

#installation for tensorflow
    #import os
    try:
        import pyspark.tensorflow
    except:
        print('Installing tensorflow...')
        os.system('pip install tensorflow -q -q -q')
        print('Installed tensorflow')
    import tensorflow
    tensorflow_path = os.path.dirname(tensorflow.__file__)
    import pyspark
    source_path = os.path.dirname(pyspark.__file__)
    ts_folder = "tensorflow"
    try:
        tensorflow_source_path=os.path.join(source_path,ts_folder)
    except:
        pass

    import shutil
    #copying files in the tensorflow directory to the pyspark's tensorflow directory
    try:
        shutil.copytree(tensorflow_path,tensorflow_source_path)
    except:

        pass
