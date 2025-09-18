# AWS HealthOmics Container Registry Maps User Guide

## Overview

Container Registry Maps are a feature in AWS HealthOmics that enable workflows to use ECR pull through caches to access public container registries without manually replicating containers into private ECR repositories. This feature provides automatic mapping between upstream registries (like Docker Hub and Quay.io) and your private ECR repositories.

### Benefits
- Avoid manual container migration to ECR
- More reliable access than downloading from public registries at runtime
- Automatic synchronization with upstream registries
- HealthOmics can use a container registry map to predictably map a public container URI in a workflow to an ECR URI resulting in a pull through of that URI at workflow runtime.

## Prerequisites

1. AWS CLI v2 installed and configured
2. Appropriate IAM permissions for ECR and HealthOmics

## Regions
You should configure your ECR registry and HealthOmics workflows in the same region. If you will use
multiple regions then repeat these steps in each region.

## Step 1: Create Secrets Manager Secrets (For Authenticated Registries)

Some registries such as Docker Hub or private registries will require authentication. To use pull through cache, you must create a secret in Secrets Manager that contains the credentials for the registry. In these examples the region `us-east-1` is specified. You should change this as needed.

To obtain a Docker Hub token refer to https://docs.docker.com/security/access-tokens/

### Docker Hub Secret
```bash
aws secretsmanager create-secret \
    --name "ecr-pullthroughcache/docker-hub" \
    --description "Docker Hub credentials for ECR pull through cache" \
    --secret-string '{
        "username": "your-docker-username",
        "accessToken": "your-docker-access-token"
    }' \
    --region us-east-1
```

### Quay.io Secret (if using private repositories, not required for public repositories)
```bash
aws secretsmanager create-secret \
    --name "ecr-pullthroughcache/quay" \
    --description "Quay.io credentials for ECR pull through cache" \
    --secret-string '{
        "username": "your-quay-username",
        "accessToken": "your-quay-access-token"
    }' \
    --region us-east-1
```

## Step 2: Create ECR Pull Through Cache Rules

### Docker Hub Pull Through Cache
```bash
aws ecr create-pull-through-cache-rule \
    --ecr-repository-prefix docker-hub \
    --upstream-registry-url registry-1.docker.io \
    --credential-arn arn:aws:secretsmanager:us-east-1:123456789012:secret:ecr-pullthroughcache/docker-hub-AbCdEf \
    --region us-east-1
```

### Quay.io Pull Through Cache
```bash
aws ecr create-pull-through-cache-rule \
    --ecr-repository-prefix quay \
    --upstream-registry-url quay.io \
    --region us-east-1
```

### ECR Public Pull Through Cache
```bash
aws ecr create-pull-through-cache-rule \
    --ecr-repository-prefix ecr-public \
    --upstream-registry-url public.ecr.aws \
    --region us-east-1
```

## Step 3: Configure Registry Permissions

Create a registry permissions policy to allow HealthOmics to use pull through cache:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowPTCinRegPermissions",
            "Effect": "Allow",
            "Principal": {
                "Service": "omics.amazonaws.com"
            },
            "Action": [
                "ecr:CreateRepository",
                "ecr:BatchImportUpstreamImage"
            ],
            "Resource": [
                "arn:aws:ecr:us-east-1:123456789012:repository/docker-hub/*",
                "arn:aws:ecr:us-east-1:123456789012:repository/quay/*",
                "arn:aws:ecr:us-east-1:123456789012:repository/ecr-public/*"
            ]
        }
    ]
}
```

Apply the policy:
```bash
aws ecr put-registry-policy \
    --policy-text file://registry-policy.json \
    --region us-east-1
```

## Step 4: Create Repository Creation Templates

### Docker Hub Template
```bash
aws ecr put-repository-creation-template \
    --prefix docker-hub \
    --applied-for PULL_THROUGH_CACHE \
    --repository-policy '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PTCRepoCreationTemplate",
                "Effect": "Allow",
                "Principal": {
                    "Service": "omics.amazonaws.com"
                },
                "Action": [
                    "ecr:BatchGetImage",
                    "ecr:GetDownloadUrlForLayer"
                ],
                "Resource": "*"
            }
        ]
    }' \
    --region us-east-1
```

### Quay.io Template
```bash
aws ecr put-repository-creation-template \
    --prefix quay \
    --applied-for PULL_THROUGH_CACHE \
    --repository-policy '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PTCRepoCreationTemplate",
                "Effect": "Allow",
                "Principal": {
                    "Service": "omics.amazonaws.com"
                },
                "Action": [
                    "ecr:BatchGetImage",
                    "ecr:GetDownloadUrlForLayer"
                ],
                "Resource": "*"
            }
        ]
    }' \
    --region us-east-1
```

### ECR Public Template
```bash
aws ecr put-repository-creation-template \
    --prefix ecr-public \
    --applied-for PULL_THROUGH_CACHE \
    --repository-policy '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PTCRepoCreationTemplate",
                "Effect": "Allow",
                "Principal": {
                    "Service": "omics.amazonaws.com"
                },
                "Action": [
                    "ecr:BatchGetImage",
                    "ecr:GetDownloadUrlForLayer"
                ],
                "Resource": "*"
            }
        ]
    }' \
    --region us-east-1
```

## Step 5: Create Container Registry Maps

### Registry Mappings Example

Registry mappings can be used to map specific upstream registries to your private ECR repositories. In the example here, containers from Docker Hub, Quay.io and ECR Public used in a workflow will be
mapped to your private ECR pull through caches.

Create a registry map file (`registry-map.json`):

```json
{
    "registryMappings": [
        {
            "upstreamRegistryUrl": "registry-1.docker.io",
            "ecrRepositoryPrefix": "docker-hub"
        },
        {
            "upstreamRegistryUrl": "quay.io",
            "ecrRepositoryPrefix": "quay"
        },
        {
            "upstreamRegistryUrl": "public.ecr.aws",
            "ecrRepositoryPrefix": "ecr-public"
        }
    ]
}
```

### Image Mappings Example

Image mappings can be used to map specific containers to your private ECR repositories. These mappings will take precedence over `registryMappings` if both are provided.

Create an image map file (`image-map.json`) for specific container overrides:

```json
{
    "imageMappings": [
        {
            "sourceImage": "broadinstitute/gatk:4.6.0.2",
            "destinationImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/docker-hub/broadinstitute/gatk:latest"
        },
        {
            "sourceImage": "quay.io/biocontainers/samtools:1.17--h00cdaf9_0",
            "destinationImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/quay/biocontainers/samtools:1.17--h00cdaf9_0"
        }
    ]
}
```

### Combined Registry and Image Map
Create a complete map file (`container-registry-map.json`):

```json
{
    "registryMappings": [
        {
            "upstreamRegistryUrl": "registry-1.docker.io",
            "ecrRepositoryPrefix": "docker-hub"
        },
        {
            "upstreamRegistryUrl": "quay.io",
            "ecrRepositoryPrefix": "quay"
        }
    ],
    "imageMappings": [
        {
            "sourceImage": "ubuntu",
            "destinationImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/docker-hub/library/ubuntu:20.04"
        },
        {
            "sourceImage": "quay.io/biocontainers/bwa:0.7.17--hed695b0_7",
            "destinationImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/quay/biocontainers/bwa:0.7.17--hed695b0_7"
        }
    ]
}
```

## Step 6: Configure HealthOmics Service Role

The HealthOmics service role used during workflow runs must have ECR permissions to pull container images from your pull through cache repositories.

### Create Trust Policy File
```bash
cat > trust-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "omics.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
```

### Create Service Role Policy File
```bash
cat > service-role-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::your-workflow-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-workflow-bucket"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:DescribeLogStreams",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:CreateLogGroup"
            ],
            "Resource": [
                "arn:aws:logs:us-east-1:123456789012:log-group:/aws/omics/WorkflowLog*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ecr:BatchGetImage",
                "ecr:GetDownloadUrlForLayer",
                "ecr:BatchCheckLayerAvailability"
            ],
            "Resource": [
                "arn:aws:ecr:us-east-1:123456789012:repository/docker-hub/*",
                "arn:aws:ecr:us-east-1:123456789012:repository/quay/*",
                "arn:aws:ecr:us-east-1:123456789012:repository/ecr-public/*"
            ]
        }
    ]
}
EOF
```

### Create the Service Role
```bash
aws iam create-role \
    --role-name HealthOmicsWorkflowRole \
    --assume-role-policy-document file://trust-policy.json \
    --description "Service role for HealthOmics workflows with container registry mappings"
```

### Create and Attach the Policy
```bash
aws iam create-policy \
    --policy-name HealthOmicsWorkflowPolicy \
    --policy-document file://service-role-policy.json \
    --description "Policy for HealthOmics workflows with ECR pull through cache access"

aws iam attach-role-policy \
    --role-name HealthOmicsWorkflowRole \
    --policy-arn arn:aws:iam::123456789012:policy/HealthOmicsWorkflowPolicy
```

### Get Role ARN for Workflow Creation
```bash
aws iam get-role --role-name HealthOmicsWorkflowRole --query 'Role.Arn' --output text
```

## Step 7: Create Workflows with Container Registry Maps

### Method 1: Inline Container Registry Map
```bash
aws omics create-workflow \
    --name "genomics-pipeline-with-ptc" \
    --description "Genomics workflow using pull through cache" \
    --engine WDL \
    --definition-zip fileb://workflow.zip \
    --container-registry-map '{
        "registryMappings": [
            {
                "upstreamRegistryUrl": "registry-1.docker.io",
                "ecrRepositoryPrefix": "docker-hub"
            },
            {
                "upstreamRegistryUrl": "quay.io",
                "ecrRepositoryPrefix": "quay"
            }
        ]
    }' \
    --region us-east-1
```

### Method 2: Container Registry Map from S3

You can use a container registry map stored in S3 as well. This is convenient if you want to use the same map in multiple workflows.

First, upload the map to S3:
```bash
aws s3 cp container-registry-map.json s3://my-workflow-bucket/maps/container-registry-map.json
```

Then create the workflow:
```bash
aws omics create-workflow \
    --name "genomics-pipeline-with-s3-map" \
    --description "Genomics workflow using S3-stored registry map" \
    --engine WDL \
    --definition-zip fileb://workflow.zip \
    --container-registry-map-uri "s3://my-workflow-bucket/maps/container-registry-map.json" \
    --region us-east-1
```

## Step 8: Example Workflow Definitions

The following workflow example contains tasks that use containers from Docker Hub, and Quay.io. These containers will be automatically mapped to your private ECR pull through caches if you create the workflow with the appropriate container registry map.

### WDL Workflow Example
```wdl
version 1.0

workflow GenomicsWorkflow {
    input {
        File input_bam
        String sample_name
    }

    call SamtoolsSort {
        input:
            input_bam = input_bam,
            sample_name = sample_name
    }

    call GatkHaplotypeCaller {
        input:
            sorted_bam = SamtoolsSort.sorted_bam,
            sample_name = sample_name
    }

    output {
        File vcf_file = GatkHaplotypeCaller.vcf
    }
}

task SamtoolsSort {
    input {
        File input_bam
        String sample_name
    }

    command <<<
        samtools sort ~{input_bam} -o ~{sample_name}.sorted.bam
    >>>

    runtime {
        docker: "quay.io/biocontainers/samtools:1.17--h00cdaf9_0"
        memory: "4 GB"
        cpu: 2
    }

    output {
        File sorted_bam = "~{sample_name}.sorted.bam"
    }
}

task GatkHaplotypeCaller {
    input {
        File sorted_bam
        String sample_name
    }

    command <<<
        gatk HaplotypeCaller \
            -I ~{sorted_bam} \
            -O ~{sample_name}.vcf \
            -R /opt/broad/references/hg38/v0/Homo_sapiens_assembly38.fasta
    >>>

    runtime {
        docker: "broadinstitute/gatk:4.6.0.2"
        memory: "8 GB"
        cpu: 4
    }

    output {
        File vcf = "~{sample_name}.vcf"
    }
}
```

## What Happens During Workflow Execution

When a workflow runs with container registry maps:

1. **Container Resolution**: HealthOmics examines each container reference in the workflow
2. **Registry Mapping**: If a registry mapping exists, the service maps the upstream registry URL to the ECR repository prefix
3. **Image Mapping**: If specific image mappings exist, they override registry mappings for those containers
4. **Repository Creation**: ECR creates the repository using the repository creation template if it doesn't exist
5. **Pull Through Cache**: ECR automatically pulls the image from the upstream registry if not already cached
6. **Task Execution**: The workflow task runs using the cached container from your private ECR registry

### Example Container Resolution

Original workflow reference: `quay.io/biocontainers/samtools:1.17--h00cdaf9_0`

With registry mapping:
- Upstream: `quay.io`
- ECR Prefix: `quay`
- Resolved: `123456789012.dkr.ecr.us-east-1.amazonaws.com/quay/biocontainers/samtools:1.17--h00cdaf9_0`

## Monitoring and Troubleshooting

### Check Pull Through Cache Rules
```bash
aws ecr describe-pull-through-cache-rules --region us-east-1
```

### Verify ECR Registry Policy Allows HealthOmics
```bash
aws ecr get-registry-policy
```

### Verify ECR Repository Creation Templates Grant Access to HealthOmics
```bash
aws ecr describe-repository-creation-templates
```

### Validate Repository Creation
```bash
aws ecr describe-repositories --repository-names docker-hub/broadinstitute/gatk --region us-east-1
```

### Validate Repository Policy Grants Access to HealthOmics
```bash
aws ecr get-repository-policy --repository-name docker-hub/broadinstitute/gatk
```

### Extract Container Registry Map from a Workflow
```bash
aws omics get-workflow --id <workflow-id> --query 'containerRegistryMap' --output json --region us-east-1
```

### Monitor Workflow Runs
```bash
aws omics get-run --id <run-id> --region us-east-1
```

### Check Task Container Details
```bash
aws omics get-run-task --id <run-id> --task-id <task-id> --region us-east-1
```

## Best Practices

1. **Use Registry Mappings**: Prefer registry mappings over image mappings for broader coverage
2. **Consistent Prefixes**: Use consistent repository prefixes across your organization
3. **Monitor Costs**: Pull through cache incurs standard ECR storage costs
4. **Version Pinning**: Always use specific container versions in workflows, for the highest level of reproducibility use SHA sums in container URIs
5. **Test Mappings**: Validate container registry maps before production use
6. **Documentation**: Document your registry mapping strategy for your team

## Security Considerations

1. **Secrets Management**: Store registry credentials securely in AWS Secrets Manager
2. **IAM Permissions**: Use least privilege principles for ECR and HealthOmics permissions
3. **Registry Policies**: Restrict access to specific repository prefixes
4. **Image Scanning**: Enable ECR image scanning for security vulnerabilities
5. **Access Logging**: Enable CloudTrail logging for ECR and HealthOmics API calls
