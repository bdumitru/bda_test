import 'dotenv/config';

const awsAccId = process.env.AWS_ACCOUNT_ID;
const bdaProjectArn = process.env.BDA_PROJECT_ARN;
const s3BucketName = process.env.BDA_S3_BUCKET;
const awsRegion = process.env.AWS_REGION;
const bdaOutputFolder = String(process.env.BDA_OUTPUT_FOLDER || 'bdaout').replace(/(?:^\s*\/|\/\s*$)/ig, '')

export {
    awsAccId,
    bdaProjectArn,
    s3BucketName,
    awsRegion,
    bdaOutputFolder
};