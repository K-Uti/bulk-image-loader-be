import amqp from 'amqplib';
import dotenv from 'dotenv';
import path from 'path';
import fs from 'fs';
import { downloadFileFromS3 } from './s3';

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

			try {
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

				console.log(
					`[Worker] 1. Downloading archive from MinIO S3 (${s3Key})...`
				);
				await downloadFileFromS3(s3Key, localTempPath);
				console.log(
					`[Worker] Successfully downloaded to local path: ${localTempPath}`
				);

				// TODO: next step is to unzip
				// and store images to DB

				console.log('[Worker] Imitate long unpacking (3 sec)...');
				await new Promise(resolve => setTimeout(resolve, 3000));

				// Remove temp archive
				if (fs.existsSync(localTempPath)) {
					fs.unlinkSync(localTempPath);
				}

				// 4. Confirm RabbitMQ that task is done
				channel.ack(msg);
				console.log('[Worker] ✅ Task is done! Message removed from queue');
				console.log('--------------------------------------------------\n');
			} catch (processingError) {
				console.error(
					'[Worker] ❌ Error while processig task:',
					processingError
				);

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
