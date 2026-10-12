import {
	S3Client,
	GetObjectCommand,
	PutObjectCommand,
	DeleteObjectCommand,
} from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';

dotenv.config();

export const s3Client = new S3Client({
	endpoint: process.env.S3_ENDPOINT,
	region: process.env.S3_REGION,
	credentials: {
		accessKeyId: process.env.S3_ACCESS_KEY!,
		secretAccessKey: process.env.S3_SECRET_KEY!,
	},
	forcePathStyle: process.env.ENV === 'local' ? true : false, // for local MinIO,
});

export async function downloadFileFromS3(
	s3Key: string,
	downloadPath: string
): Promise<void> {
	const command = new GetObjectCommand({
		Bucket: process.env.S3_BUCKET_NAME!,
		Key: s3Key,
	});
	console.log('COMMAND: ', command);

	const response = await s3Client.send(command);

	console.log('RESPONSE: ', response);

	const stream = response.Body as Readable;

	// create folder if none
	fs.mkdirSync(path.dirname(downloadPath), { recursive: true });

	return new Promise((resolve, reject) => {
		const fileStream = fs.createWriteStream(downloadPath);
		stream.pipe(fileStream);
		stream.on('error', reject);
		fileStream.on('finish', resolve);
	});
}

// load image buffer to s3
export async function uploadFileToS3(
	buffer: Buffer,
	s3Key: string,
	contentType: string
): Promise<string> {
	const bucketName = process.env.S3_BUCKET_NAME || 'auction-bucket';

	await s3Client.send(
		new PutObjectCommand({
			Bucket: bucketName,
			Key: s3Key,
			Body: buffer,
			ContentType: contentType,
		})
	);

	return s3Key;
}

// delete initially uploaded archive to MinIO
export async function deleteFileFromS3(s3Key: string): Promise<void> {
	const bucketName = process.env.S3_BUCKET_NAME;
	await s3Client.send(
		new DeleteObjectCommand({ Bucket: bucketName, Key: s3Key })
	);
	console.log(`[S3] Initial archive is deleted: ${s3Key}`);
}
