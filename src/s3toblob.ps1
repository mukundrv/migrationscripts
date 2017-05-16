#Set-ExecutionPolicy RemoteSigned
#Get-Module -ListAvailable

#Read-S3Object -BucketName YOURBUCKETNAME -Key FILENAME -File c:\FILENAME

Initialize-AWSDefaults -ProfileName default -Region us-east-1

# Your account access key - must have read access to your S3 Bucket
$accessKey = "<ACCESSkey>"
# Your account secret access key
$secretKey = "<Secret KEy>"
# The region associated with your bucket e.g. eu-west-1, us-east-1 etc. (see http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-regions)
$region = "us-west-2"
# The name of your S3 Bucket
$bucket = "gcp-datasets"
# The folder in your bucket to copy, including trailing slash. Leave blank to copy the entire bucket
$keyPrefix = "<Prefix>"
# The local file path where files should be copied
$localPath = "C:\Users\bmuser\Desktop\s3"    
Set-DefaultAWSRegion -Region us-east-1
Write-Host $(Get-DefaultAWSRegion)



$localFilePathPrefix = "C:\Users\bmuser\Downloads\S3\"


$SubscriptionName = "Pay-As-You-Go"
$StorageAccountName = "<Accountname>"
$StorageAccountKey = "<Storage key>"
$ContainerName = "<Container name>"

$location = "West US"

$ctx = New-AzureStorageContext -StorageAccountName $StorageAccountName -StorageAccountKey $StorageAccountKey 



$prefix= "tpch_10tb/lineitem/"
$file="lineitem-"

$start = 400
$end =499

$logfilepath = $localFilePathPrefix + "thread1.log"

for($i=$start; $i -le $end; $i++) {
    $filetoDownload = $file + $i.ToString("000000000000") + ".gz"
    $key= $prefix + $filetoDownload
    $localFilePath= $localFilePathPrefix + $filetoDownload
    #Add-Content $logfilepath "Downloading file " $key "to" $localFilePath
    
    Copy-S3Object -BucketName $bucket -Key $key -LocalFile $localFilePath 

    Write-Host "Uploading file " $localFilePath "to" $ContainerName

    Set-AzureStorageBlobContent -Container $ContainerName  -Context $ctx -File $localFilePath 

    Write-Host "Deleting file " $localFilePath 

    Remove-Item $localFilePath
    
}

<#
$objects = Get-S3Object -BucketName $bucket -KeyPrefix $keyPrefix 

foreach($object in $objects) {
    #Write-Host $object.Key
    $localFileName = $object.Key -replace $keyPrefix, ''
    if ($localFileName -ne '') {
        $localFilePath = Join-Path $localPath $localFileName
 #       Copy-S3Object -BucketName $bucket -Key $object.Key -LocalFile $localFilePath 
    }
}

#>


