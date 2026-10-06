import type { Job } from "bullmq";

import type { KTJobData, KTJobPayload } from "../bullmq/job.js";
import type { KTTopicBatchPayload } from "../kafka/topic-batch.js";
import type { KTTopicPayloadWithMeta } from "../kafka/topic.js";

export type KTPublisher = {
  publishSingleMessage: (topic: KTTopicPayloadWithMeta) => Promise<void>
  publishBatchMessages: (topic: KTTopicBatchPayload) => Promise<void>
  publishJob: (job: KTJobPayload) => Promise<Job<KTJobData, void, string>>
  publishBatchJobs: (jobs: KTJobPayload[]) => Promise<void>
}
