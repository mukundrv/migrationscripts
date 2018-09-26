#!/usr/bin/env python3

import boto3
import json

s3 = boto3.client('s3')
s3R = boto3.resource('s3')

print 'start'
bucket = '<bucketname>'
prefix = '<prefix>'
manifestOutputFolder  = '<klocal folder temp>'
manifestFile = '<manifest name>'


def createManifest():

	print "list objects"
	responses= s3.list_objects(Bucket=bucket,Prefix=prefix)

	print "received response"
	manifest = {"entries":[]}

	#print(len(manifest['entries']))

	#print responses

	for content in responses['Contents']:
		#print content['Key']
		if(content['Key'] != (prefix +  manifestFile)):
			
			print(content['Key'])
			if(not str(content['Key']).endswith('/')):
				item = {}
				item['url'] = "s3://"+bucket+"/"+content['Key']
				item['mandatory'] = True

				manifest['entries'].append(item);

	print "list objects complete"
	return manifest

def main():

	manifest = createManifest()

	with open((manifestOutputFolder + manifestFile), 'w') as outfile:
		json.dump(manifest,outfile,indent=4, separators=(',', ': '))

	print(json.dumps(manifest,indent=4, separators=(',', ': ')))

	print("dump complete")

	
	s3.upload_file((manifestOutputFolder + manifestFile),bucket, prefix+manifestFile)
	print ("Manifest file = \n" + ("s3:" + "//" + bucket + "/" +   prefix +  manifestFile))
	print("upload complete")

	return

main()

print 'end'
'''for bucket in s3.buckets.all():
	if bucket.name == 'gcp-datasets':
		print bucket.name
		for key in bucket.objects.all():
			print key



'''
