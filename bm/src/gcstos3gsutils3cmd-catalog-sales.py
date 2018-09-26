from google.cloud import storage
from os.path import *
from os import listdir
from os.path import isfile, join

import os
import sys
import glob
import subprocess
import contextlib
import functools
import multiprocessing
from multiprocessing.pool import IMapIterator
from optparse import OptionParser

import datetime
import time
import threading

import boto

conn = boto.connect_s3('<acceskey>','<secretkey>')
bucket = conn.lookup(S3_BUCKET)

GS_SOURCE_BUCKET_NAME = "<gs_bucket_nae>"
GS_PREFIX = "<prefix>"

S3_BUCKET = "<s3bucket>"
S3_PREFIX = "<s3prefix>"

ITEMS_TO_REMOVE = ["store_sales_100_180.dat","store_sales_101_180.dat","store_sales_102_180.dat","store_sales_103_180.dat","store_sales_104_180.dat","store_sales_105_180.dat","store_sales_106_180.dat","store_sales_107_180.dat","store_sales_108_180.dat","store_sales_109_180.dat","store_sales_110_180.dat","store_sales_10_180.dat","store_sales_112_180.dat","store_sales_114_180.dat"]
OUTPUT_FOLDER = "./"


def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    print("List all blobs in the bucket")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=GS_PREFIX)
    blobarray = []
    # print blobs
    for blob in blobs:
        #print(blob.name)
        blobarray.append(blob.name)
    print(len(blobarray))
    for item in ITEMS_TO_REMOVE:
        # print(item)
        try:
            blobarray.remove(join(GS_PREFIX, item))
        except Exception, e:
            print("Not in list:", str(e))
    print(len(blobarray))
    return blobarray


def chunkIt(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out


def download_blob(source_blob_name, destination_file_name):
    print("""Downloads a blob from the bucket.""")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(GS_SOURCE_BUCKET_NAME)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        destination_file_name))


def download_blob_using_gsutil(source_blob_name, destination_file_name):
    print(source_blob_name + " " + destination_file_name)
    blobfullpath = join("gs://",GS_SOURCE_BUCKET_NAME, source_blob_name )
    print(blobfullpath)
    cl = ["gsutil", "-m" , "cp", blobfullpath ,destination_file_name]
    print("test - ---- 3")
    subprocess.check_call(cl)

def main():

        # len(blobs)
        #print(list(chunks(range(10, 75), 5)))
        # return

    blobunsplit = list_blobs(GS_SOURCE_BUCKET_NAME)
    blobsAfterSplit = chunkIt(blobunsplit, 5)
    print(str(len(blobsAfterSplit[0])) +" --- " + str(len(blobsAfterSplit[4])))

    blobmap0 = {'blobs':blobsAfterSplit[0]}
    blobmap1 = {'blobs':blobsAfterSplit[1]}
    blobmap2 = {'blobs':blobsAfterSplit[2]}
    blobmap3 = {'blobs':blobsAfterSplit[3]}
    blobmap4 = {'blobs':blobsAfterSplit[4]}
    # print(blobsAfterSplit)
    #blobs = blobunsplit[:]
    #print(blobs)
    threads = []
    try:
        t1 = threading.Thread(name="t1", target=processFiles, kwargs= blobmap0)
        t2 = threading.Thread(name="t2", target=processFiles, kwargs= blobmap1)
        t3 = threading.Thread(name="t3", target=processFiles, kwargs= blobmap2)
        t4 = threading.Thread(name="t4", target=processFiles, kwargs= blobmap3)
        t5 = threading.Thread(name="t5", target=processFiles, kwargs= blobmap4)
        threads.append(t1)
        threads.append(t2)
        threads.append(t3)
        threads.append(t4)
        threads.append(t5)
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        t5.start()
        #p1 = thread.start_new_thread(processKinesis, ('xaa', 1))
    except Exception, e:
        print("Unexpected error in main:", str(e))

    for t in threads:
        t.join()

def processFiles(blobs=None):
    for bx in blobs:
        downloadComplete = False
        uploadComplete = False
        filee = bx.split("/")
        filename = filee[len(filee) - 1]
        outputfile = join(OUTPUT_FOLDER, filename)
        try:
            print("Transferring " + outputfile)
            download_blob_using_gsutil(bx, outputfile)
            downloadComplete = True
            print(" Download complete for  " + filename)
        except Exception, e:
            print("Error while downloading from GS:", str(e))

        if downloadComplete:
            #try:
                print("Uploading to S3 "+filename)
                uploadToS3(outputfile)
                uploadComplete = True
                print("Upload complete for " + filename)
            #except Exception, e:
            #    print("Error while uploading to S3:", str(e))

        if uploadComplete:
            try:
                if(os.path.exists(filename)):
                    os.remove(filename)
            except Exception, e:
                print("Error while deleting file ", str(e))
        #break                                       

def uploadToS3(transfer_file):

    s3_key_name = os.path.basename(transfer_file)
    
    s3_key = bucket.get_key(s3_key_name)
    print(transfer_file)
    print(s3_key_name)
    s3fullname = join("s3://",S3_BUCKET,S3_PREFIX,s3_key_name)
    print(s3fullname)
    cl = ["s3cmd", "put" , s3_key_name, s3fullname ]
    print("test - ---- 3")
    subprocess.check_call(cl)

main()

                 
