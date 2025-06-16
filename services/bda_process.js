import { setTimeout } from 'timers/promises';
import { s3BucketName, awsRegion, bdaProjectArn, bdaOutputFolder, awsAccId } from '../config.js';
import { BedrockDataAutomationRuntimeClient, InvokeDataAutomationAsyncCommand, GetDataAutomationStatusCommand } from '@aws-sdk/client-bedrock-data-automation-runtime';

const bdaRuntimeClient = new BedrockDataAutomationRuntimeClient({ region: awsRegion });

const invokeBda = async (fileLocationInBucket, projectArn) => {
    const bdaConfig = {
        inputConfiguration: {
            s3Uri: `s3://${s3BucketName}/${fileLocationInBucket}`
        },
        outputConfiguration: {
            s3Uri: `s3://${s3BucketName}/${bdaOutputFolder}`
        },
        dataAutomationConfiguration: {
            dataAutomationProjectArn: `arn:aws:bedrock:${awsRegion}:${awsAccId}:data-automation-project/${projectArn}`
        },
        dataAutomationProfileArn: `arn:aws:bedrock:${awsRegion}:${awsAccId}:data-automation-profile/us.data-automation-v1`
    }
    const initiateBdaCommand = new InvokeDataAutomationAsyncCommand(bdaConfig);
    const bdaInitResp = await bdaRuntimeClient.send(initiateBdaCommand);

    return bdaInitResp;
};

const getBdaStatus = async (invocationArn) => {
    const getBdaStatusCommand = new GetDataAutomationStatusCommand({ invocationArn });
    const bdaStatusResp = await bdaRuntimeClient.send(getBdaStatusCommand);

    return bdaStatusResp;
};

const executeBda = async (fileLocationInBucket, projectArn = bdaProjectArn) => {
    const invokeResp = await invokeBda(fileLocationInBucket, projectArn);
    const invocationArn = invokeResp.invocationArn;

    let bdaStatusResp = await getBdaStatus(invocationArn);

    while (bdaStatusResp.status !== 'Success' && bdaStatusResp.status && !bdaStatusResp.status.includes('Error')) {
        await setTimeout(1000);
        bdaStatusResp = await getBdaStatus(invocationArn);
    };

    if (bdaStatusResp.status !== 'Success') {
        const bdaError = new Error(bdaStatusResp.errorMessage || 'BDA response could not be fetched');
        bdaError.bdaResponse = bdaStatusResp
        throw bdaError;
    };

    return bdaStatusResp.outputConfiguration.s3Uri;
};

export default executeBda;