import {
	pgTable,
	uuid,
	varchar,
	integer,
	timestamp,
} from 'drizzle-orm/pg-core';
import { relations } from 'drizzle-orm';

export enum BatchStatus {
	IDLE = 'idle',
	UPLOADING = 'uploading',
	PROCESSING = 'processing',
	SUCCESS = 'success',
	FAILED = 'failed',
}

// Archives
export const batches = pgTable('batches', {
	id: uuid('id').defaultRandom().primaryKey(),
	name: varchar('name', { length: 255 }).notNull(),
	status: varchar('status', { length: 50 }).default(BatchStatus.IDLE).notNull(), // idle, uploading, processing, success, failed
	progress: integer('progress').default(0).notNull(),
	totalItems: integer('total_items').default(0).notNull(),
	createdAt: timestamp('created_at').defaultNow().notNull(),
});

// Lots/images
export const lots = pgTable('lots', {
	id: uuid('id').defaultRandom().primaryKey(),
	batchId: uuid('batch_id')
		.references(() => batches.id, { onDelete: 'cascade' })
		.notNull(),
	title: varchar('title', { length: 255 }).notNull(),
	imageUrl: varchar('image_url', { length: 512 }).notNull(), // link to MinIO S3
	createdAt: timestamp('created_at').defaultNow().notNull(),
});

export const batchesRelations = relations(batches, ({ many }) => ({
	lots: many(lots),
}));

export const lotsRelations = relations(lots, ({ one }) => ({
	batch: one(batches, {
		fields: [lots.batchId],
		references: [batches.id],
	}),
}));
