"""A containerized Worker needs config (Temporal address/namespace, task queue,
downstream endpoints) and secrets (an API key) without baking them into the image
or shipping them as plaintext env vars. In AWS this is "env vars / mounted secrets
file"; here it's SSM Parameter Store read at WORKER STARTUP. get_parameters_by_path
loads a whole "/temporal-training/worker/" tree at boot, WithDecryption decrypts
SecureString params via KMS, and the values build the Temporal Client + Worker.

The boundary that matters for Temporal: this read happens in plain process
(bootstrap) code, NOT in a Workflow — an SSM call is non-deterministic I/O and the
value can change between replays. When a Workflow STEP needs a secret, it reads it
inside the fetch_api_key Activity, never in Workflow code. In ECS/EKS the SSM
client authenticates via the task role / IRSA, so there are no static keys.

The Python port of ssm_parameter_config.java. boto3 is imported lazily so this
module py_compiles without the AWS SDK; the client points at LocalStack to mirror
the Glue/SNS client config in the earlier labs.
"""

from dataclasses import dataclass

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker


def _ssm_client():
    import boto3  # lazy: AWS SDK may be absent offline

    # Point at LocalStack; in ECS/EKS drop endpoint_url and let the task role /
    # IRSA supply credentials.
    return boto3.client(
        "ssm",
        endpoint_url="http://127.0.0.1:4566",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@dataclass(frozen=True)
class WorkerConfig:
    temporal_address: str
    namespace: str
    task_queue: str
    order_api_url: str


def load_config(path: str = "/temporal-training/worker/") -> WorkerConfig:
    """Load the whole config tree at startup. WithDecryption=True decrypts any
    SecureString params via KMS; get_parameters_by_path paginates, so follow
    NextToken until it's empty to be sure every key is loaded."""
    ssm = _ssm_client()
    params: dict[str, str] = {}
    next_token = None
    while True:
        kwargs = {"Path": path, "Recursive": True, "WithDecryption": True}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = ssm.get_parameters_by_path(**kwargs)
        for p in resp["Parameters"]:
            # key by the trailing name: ".../namespace" -> "namespace"
            params[p["Name"].rsplit("/", 1)[-1]] = p["Value"]
        next_token = resp.get("NextToken")
        if not next_token:
            break

    return WorkerConfig(
        temporal_address=params["temporal-address"],
        namespace=params.get("namespace", "default"),
        task_queue=params["task-queue"],
        order_api_url=params["order-api-url"],
    )


@activity.defn
async def fetch_api_key() -> str:
    """A Workflow step that needs the API key calls THIS, not SSM directly — the
    read is non-deterministic I/O and belongs behind the Activity boundary. The
    plaintext key stays inside the Activity; never log it and avoid returning it
    into Workflow history (pass it straight to the downstream call instead)."""
    ssm = _ssm_client()
    return ssm.get_parameter(
        Name="/temporal-training/worker/api-key",
        WithDecryption=True,  # SecureString -> decrypted via KMS
    )["Parameter"]["Value"]


async def bootstrap() -> Worker:
    """Process startup code — reading SSM here is fine. Cache the config once at
    boot, then build the Temporal Client + Worker from it; don't re-read per task."""
    cfg = load_config()
    client = await Client.connect(cfg.temporal_address, namespace=cfg.namespace)
    return Worker(
        client,
        task_queue=cfg.task_queue,
        activities=[fetch_api_key],
        # workflows=[...],
    )
