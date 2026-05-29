CREATE TABLE `workflow` (
  `id` text PRIMARY KEY NOT NULL,
  `project_id` text NOT NULL,
  `root_session_id` text,
  `pm_session_id` text,
  `tester_session_id` text,
  `request` text NOT NULL,
  `title` text NOT NULL,
  `directory` text NOT NULL,
  `path` text NOT NULL,
  `xml` text NOT NULL,
  `status` text NOT NULL,
  `model` text,
  `agent` text,
  `test_path` text,
  `error` text,
  `time_created` integer NOT NULL,
  `time_updated` integer NOT NULL,
  `time_completed` integer,
  FOREIGN KEY (`root_session_id`) REFERENCES `session`(`id`) ON UPDATE no action ON DELETE set null,
  FOREIGN KEY (`pm_session_id`) REFERENCES `session`(`id`) ON UPDATE no action ON DELETE set null,
  FOREIGN KEY (`tester_session_id`) REFERENCES `session`(`id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
CREATE INDEX `workflow_project_idx` ON `workflow` (`project_id`);
--> statement-breakpoint
CREATE INDEX `workflow_root_session_idx` ON `workflow` (`root_session_id`);
--> statement-breakpoint
CREATE INDEX `workflow_status_idx` ON `workflow` (`status`);
--> statement-breakpoint
CREATE TABLE `workflow_milestone` (
  `workflow_id` text NOT NULL,
  `id` text NOT NULL,
  `title` text,
  `department` text,
  `prompt` text NOT NULL,
  `depends_on` text NOT NULL,
  `status` text NOT NULL,
  `attempt` integer DEFAULT 0 NOT NULL,
  `plan_path` text,
  `review_path` text,
  `session` text NOT NULL,
  `time_created` integer NOT NULL,
  `time_updated` integer NOT NULL,
  PRIMARY KEY (`workflow_id`, `id`),
  FOREIGN KEY (`workflow_id`) REFERENCES `workflow`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `workflow_milestone_workflow_status_idx` ON `workflow_milestone` (`workflow_id`, `status`);
--> statement-breakpoint
CREATE TABLE `workflow_edge` (
  `workflow_id` text NOT NULL,
  `from_id` text NOT NULL,
  `to_id` text NOT NULL,
  `data` text,
  PRIMARY KEY (`workflow_id`, `from_id`, `to_id`),
  FOREIGN KEY (`workflow_id`) REFERENCES `workflow`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `workflow_edge_workflow_idx` ON `workflow_edge` (`workflow_id`);
--> statement-breakpoint
CREATE TABLE `workflow_consultation` (
  `workflow_id` text NOT NULL,
  `id` text NOT NULL,
  `from_session_id` text NOT NULL,
  `to_session_id` text NOT NULL,
  `from_role` text NOT NULL,
  `to_role` text NOT NULL,
  `milestone_id` text,
  `question` text NOT NULL,
  `answer` text NOT NULL,
  `status` text NOT NULL,
  `time_created` integer NOT NULL,
  `time_updated` integer NOT NULL,
  PRIMARY KEY (`workflow_id`, `id`),
  FOREIGN KEY (`workflow_id`) REFERENCES `workflow`(`id`) ON UPDATE no action ON DELETE cascade,
  FOREIGN KEY (`from_session_id`) REFERENCES `session`(`id`) ON UPDATE no action ON DELETE cascade,
  FOREIGN KEY (`to_session_id`) REFERENCES `session`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `workflow_consultation_workflow_idx` ON `workflow_consultation` (`workflow_id`);
--> statement-breakpoint
CREATE INDEX `workflow_consultation_from_session_idx` ON `workflow_consultation` (`from_session_id`);
--> statement-breakpoint
CREATE INDEX `workflow_consultation_to_session_idx` ON `workflow_consultation` (`to_session_id`);
