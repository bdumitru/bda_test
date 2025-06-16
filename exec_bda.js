import executeBda from './services/bda_process.js';
import { uploadFileToS3, getFileFromS3 } from './services/s3.js';
import { s3BucketName, bdaProjectArn } from './config.js';
import { readdir, readFile, writeFile } from 'fs/promises';
import { v4 as uuid } from 'uuid';

const getFilePathFromS3Url = (s3Url) => {
    return s3Url.replace(`s3://${s3BucketName}/`, '');
};

const processFile = async (fileBuffer, projectArn = bdaProjectArn) => {
    const fileUuid = uuid() + '.pdf';
    await uploadFileToS3(fileBuffer, fileUuid);
    const metadataUri = await executeBda(fileUuid, projectArn);
    const filePath = getFilePathFromS3Url(metadataUri);
    const metadataString = await getFileFromS3(filePath);
    const metadataJson = JSON.parse(metadataString);

    if (metadataJson['job_status'] !== 'PROCESSED') {
        throw Error(`Unexpected BDA status: ${metadataJson['job_status']}. Job url: ${metadataUri}`);
    };

    const resultJsons = [];
    for (const outputMetadataItem of metadataJson['output_metadata']) {
        for (let segmentIdx = 0; segmentIdx < outputMetadataItem['segment_metadata'].length; segmentIdx++) {
            if (!outputMetadataItem['segment_metadata'][segmentIdx]['custom_output_status'] || outputMetadataItem['segment_metadata'][segmentIdx]['custom_output_status'] !== 'MATCH') {
                console.warn(`No custom output for segment ${segmentIdx} of ${metadataUri}`);
            } else {
                const resultFilePath = getFilePathFromS3Url(outputMetadataItem['segment_metadata'][segmentIdx]['custom_output_path']);
                const resultString = await getFileFromS3(resultFilePath);
                const resultJson = JSON.parse(resultString);
                resultJsons.push(resultJson);
            };
        };
    };

    return resultJsons;
};


(async () => {
    const inputFolder = './files_input';
    const outputFolder = './files_output';
    const inputFiles = await readdir(inputFolder)

    for (const inputFile of inputFiles) {
        const fileBuffer = await readFile(`${inputFolder}/${inputFile}`);
        const resultJsons = await processFile(fileBuffer);
        await writeFile(`${outputFolder}/${inputFile}.json`, JSON.stringify(resultJsons));

    };
})();