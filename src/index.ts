import amqp from 'amqplib';
import dotenv from 'dotenv';
import path from 'path';
import fs from 'fs';
import { downloadFileFromS3, uploadFileToS3, deleteFileFromS3 } from './s3';

import AdmZip from 'adm-zip';
import { eq } from 'drizzle-orm';
import { db } from './db';
import { batches, lots, BatchStatus } from './db/schema';

dotenv.config();

async function startWorker() {
	try {
		console.log('[Worker] Running service for processing archives...');

		// 1. connect to RabbitMQ
		const connection = await amqp.connect(process.env.RABBITMQ_URL!);
		const channel = await connection.createChannel();

		// 2. Claim the queue in case worker started earlier then Next.js)
		const queueName = 'upload_batches';
		await channel.assertQueue(queueName, { durable: true });

		// restrict worker to finish this task before taking another
		await channel.prefetch(1);

		console.log(
			`[Worker] Successfully connected. waiting for tasks in "${queueName}"...`
		);

		// 3. Start listening to queue
		await channel.consume(queueName, async msg => {
			if (!msg) return;

			// Парсим JSON из бинарного буфера сообщения
			const taskData = JSON.parse(msg.content.toString());
			console.log('\n--------------------------------------------------');
			console.log('[Worker] New task from RabbitMQ!');
			console.log('[Worker] Data:', taskData);

			const { batchId, s3Key, fileName } = taskData;

			// Определяем, куда локально сохраним скачанный архив
			const localTempPath = path.join(
				__dirname,
				'../tmp',
				`${batchId}-${fileName}`
			);

			try {
				// 1. Update status in DB in case next didn't do it yet
				await db
					.update(batches)
					.set({ status: BatchStatus.PROCESSING, progress: 40 })
					.where(eq(batches.id, batchId));

				console.log(
					`[Worker] 1. Downloading archive from MinIO S3 (${s3Key})...`
				);
				await downloadFileFromS3(s3Key, localTempPath);
				console.log(
					`[Worker] Successfully downloaded to local path: ${localTempPath}`
				);

				// 3. Unpack archive adm-zip
				console.log('[Worker] 2. Unpacking archive');
				const zip = new AdmZip(localTempPath);
				const zipEntries = zip.getEntries(); // list of files in archive

				// Filter only images (ignore system files like __MACOSX)
				const imageEntries = zipEntries.filter(
					entry =>
						!entry.isDirectory &&
						(entry.entryName.endsWith('.jpg') ||
							entry.entryName.endsWith('.png') ||
							entry.entryName.endsWith('.jpeg'))
				);

				console.log(`[Worker] Number of image found: ${imageEntries.length}`);

				// ARRAY to collect all the lots before pushing them to DB
				const lotsToInsert: Array<{
					batchId: string;
					title: string;
					imageUrl: string;
				}> = [];

				// 4. process each image in a loop
				for (let i = 0; i < imageEntries.length; i++) {
					const entry = imageEntries[i];
					const imageBuffer = entry.getData(); // binary buffer

					// create cool name
					const lotTitle = path.parse(entry.name).name.replace(/[-_]/g, ' ');

					// store path inside MinIO
					const processedS3Key = `processed-images/${batchId}/${entry.name}`;
					const contentType = entry.name.endsWith('.png')
						? 'image/png'
						: 'image/jpeg';

					console.log(
						`[Worker] Processing image (${i + 1}/${
							imageEntries.length
						}): "${lotTitle}"`
					);

					//Save image to S3
					await uploadFileToS3(imageBuffer, processedS3Key, contentType);

					// todo: too expensive update db on each iteration
					//  create image record inside PostgreSQL via Drizzle
					// await db.insert(lots).values({
					// 	batchId: batchId,
					// 	title: lotTitle,
					// 	imageUrl: processedS3Key,
					// });
					// instead:

					lotsToInsert.push({
						batchId,
						title: lotTitle,
						imageUrl: processedS3Key,
					});

					// TODO: too expensive (maybe calculate 30% of upload 50% and 75 and update these values)
					//  calculate and update progress
					// const currentProgress = Math.round(
					// 	40 + ((i + 1) / imageEntries.length) * 50
					// ); // from 40% to 90%
					// await db
					// 	.update(batches)
					// 	.set({ progress: currentProgress })
					// 	.where(eq(batches.id, batchId));
				}

				// 5. Batch insert: one query to record all image objects at the same time
				if (lotsToInsert.length > 0) {
					console.log(
						`[Worker] Storing ${lotsToInsert.length} images to DB...`
					);
					await db.insert(lots).values(lotsToInsert);
				}

				// 6. Clear s3 storage. delete initial zip file
				console.log('[Worker] Deleting initial zip file from MinIO...');
				await deleteFileFromS3(s3Key);

				// 7. Final Update batch in DB
				await db
					.update(batches)
					.set({
						status: 'success',
						progress: 100,
						totalItems: imageEntries.length,
					})
					.where(eq(batches.id, batchId));

				// Remove temp archive
				if (fs.existsSync(localTempPath)) {
					fs.unlinkSync(localTempPath);
				}

				// 8. Confirm RabbitMQ that task is done
				channel.ack(msg);
				console.log('[Worker] ✅ Task is done! Message removed from queue');
				console.log('--------------------------------------------------\n');
			} catch (processingError) {
				console.error(
					'[Worker] ❌ Error while processig task:',
					processingError
				);

				// set status failed in DB
				await db
					.update(batches)
					.set({ status: BatchStatus.FAILED })
					.where(eq(batches.id, batchId));

				// In case of error return message to queue, not to lose data (requeue: true)
				// in real prod use DLQ (Dead Letter Queue), but for start just return to queue
				channel.nack(msg, false, true);
			}
		});
	} catch (error) {
		console.error('[Worker] Critical error running the worker:', error);
		process.exit(1);
	}
}

// Запускаем воркер
startWorker();
