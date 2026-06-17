# A minimal Aurora PostgreSQL cluster to act as the import pipeline's
# transactional SINK (the `load` Activity writes here). Placeholder values —
# tighten the security group, CIDRs, and instance class before any real use.
#
# COST: a running cluster bills by the hour (RDS, ~cents+/hr) — this is NOT
# free-LocalStack territory. `terraform destroy` when you're done with the lab.
#
# SECRETS: the master password below is wired to come from AWS Secrets Manager,
# NOT hardcoded. Even better, iam_database_authentication_enabled lets the Worker
# connect with a short-lived IAM auth token and no stored password at all.

variable "db_password" {
  description = "Aurora master password — supplied from Secrets Manager / SSM, never committed."
  type        = string
  sensitive   = true
  # In practice: data "aws_secretsmanager_secret_version" "db" { ... }
  # then default = jsondecode(...).password — do not put a literal here.
}

resource "aws_rds_cluster" "imports" {
  cluster_identifier = "temporal-imports"
  engine             = "aurora-postgresql"
  engine_version     = "15.4"
  database_name      = "imports"
  master_username    = "import_writer"
  master_password    = var.db_password # from Secrets Manager, see variable above

  # Let the Worker authenticate with a short-lived IAM token instead of a stored
  # password — the lab's "no password in code" stretch goal.
  iam_database_authentication_enabled = true

  vpc_security_group_ids = [aws_security_group.aurora.id]
  skip_final_snapshot    = true # demo only; keep a final snapshot in production
}

resource "aws_rds_cluster_instance" "imports" {
  identifier         = "temporal-imports-1"
  cluster_identifier = aws_rds_cluster.imports.id
  engine             = aws_rds_cluster.imports.engine
  instance_class     = "db.t3.medium" # smallest sensible class for a lab
  # The cluster exposes two endpoints:
  #   aws_rds_cluster.imports.endpoint         -> WRITER  (the load Activity uses this)
  #   aws_rds_cluster.imports.reader_endpoint  -> READER  (read-only queries/replicas)
}

# Security group note: this is intentionally permissive for a lab. Lock ingress
# down to the Worker's subnet/SG (e.g. the EKS node security group), never
# 0.0.0.0/0, before running anything real.
resource "aws_security_group" "aurora" {
  name        = "temporal-imports-aurora"
  description = "Aurora access for the Temporal import Worker"

  ingress {
    description = "Postgres from the Worker only — replace with the Worker's SG/CIDR"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"] # placeholder: the VPC CIDR, NOT the open internet
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

output "writer_endpoint" {
  description = "Point the Worker's HikariCP JDBC URL here — writes go to the writer."
  value       = aws_rds_cluster.imports.endpoint
}

output "reader_endpoint" {
  description = "Read-only endpoint for queries/reporting; not used by the load Activity."
  value       = aws_rds_cluster.imports.reader_endpoint
}
