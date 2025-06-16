import { s3BucketName, awsRegion } from '../config.js';
import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';

if (!awsRegion || !s3BucketName) {
    throw Error('Please set BDA_S3_BUCKET, AWS_REGION env vars.');
};

const s3client = new S3Client({ regio: awsRegion });

const uploadFileToS3 = async (fileBuffer, uploadPath) => {
    const uploadParams = {
        Body: fileBuffer,
        Bucket: s3BucketName,
        Key: uploadPath
    };
    const uploadCommand = new PutObjectCommand(uploadParams);
    const uploadResp = await s3client.send(uploadCommand);
    return uploadResp;
};

const getFileFromS3 = async (filePath) => {
    const downloadParams = {
        Bucket: s3BucketName,
        Key: filePath
    };
    const downloadCommand = new GetObjectCommand(downloadParams);
    const downloadResp = await s3client.send(downloadCommand);

    const stringResponse = await downloadResp.Body.transformToString();
    return stringResponse;
};

export {
    uploadFileToS3,
    getFileFromS3
};