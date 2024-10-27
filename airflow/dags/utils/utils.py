from hdfs import InsecureClient

def hdfs_upload(input_path, output_path):
    client = InsecureClient('http://namenode:9870')
    client.upload(output_path, input_path, overwrite=True)