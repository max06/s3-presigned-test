#!/usr/bin/env python

# Main stuff
from omegaconf import DictConfig, OmegaConf
import hydra
from loguru import logger
import yaml

# S3 related stuff
import boto3
import requests
import http.client as http_client


@hydra.main(config_path="", config_name="config")
def app(cfg: DictConfig) -> None:
    """
    Main function.
    """
    logger.info("Test starting...")

    http_client.HTTPConnection.debuglevel = 1

    # Setup boto3-client
    s3 = boto3.client(
        's3',
        endpoint_url=cfg.endpoint,
        aws_access_key_id=cfg.aws_access_key_id,
        aws_secret_access_key=cfg.aws_secret_access_key
    )

    object_name = 'testfile.txt'

    presigned = s3.generate_presigned_post(
        cfg.bucket,
        object_name,
        Conditions=[
            {"acl": "public-read"}
        ],
        Fields={"acl": "public-read"}
    )

    logger.debug("Presigned object: \n{}", yaml.dump(presigned))

    # Demonstrate how another Python program can use the presigned URL to upload a file
    with open("/workspaces/s3-presigned-test/" + object_name, 'rb') as f:
        files = {'file': (object_name, f)}
        logger.debug("Files: {}", files)

        upload = requests.post(
            presigned["url"],
            data=presigned["fields"],
            files=files
        )

        # If successful, returns HTTP status code 204
        logger.debug("Upload: {}", upload.status_code)

    logger.info("Test finished!")


if __name__ == "__main__":
    app()
